package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/metrics"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

const (
	quotaCrashGapSeconds   = 30
	quotaRepairEventType   = "QUOTA_REPAIR"
	quotaRepairSystemAdmin = "00000000-0000-0000-0000-000000000001"
	quotaRepairTargetType  = "campaign_quota"
)

// QuotaRepairAction is the PG-authoritative repair decision applied via outbox.
type QuotaRepairAction string

const (
	QuotaRepairTopUpRedis QuotaRepairAction = "topup_redis"
	QuotaRepairReleasePG  QuotaRepairAction = "release_pg"
)

// QuotaRepairPayload is the outbox body for QUOTA_REPAIR (M3).
type QuotaRepairPayload struct {
	CampaignID    string `json:"campaign_id"`
	ShardID       int16  `json:"shard_id"`
	Action        string `json:"action"`
	PgReserved    int64  `json:"pg_reserved"`
	RedisExpected int64  `json:"redis_expected"`
	ChunkSize     int64  `json:"chunk_size"`
	DriftMicro    int64  `json:"drift_micro"`
	RepairMicro   int64  `json:"repair_micro"`
	Reason        string `json:"reason"`
}

type quotaRow struct {
	shardID        int16
	campaignID     uuid.UUID
	reservedAmount int64
	chunkSize      int64
	updatedAt      time.Time
}

// RepairQuotaDrift scans PG↔Redis quota drift and crash gaps; enqueues QUOTA_REPAIR when enabled.
func (w *ReconWorker) RepairQuotaDrift(ctx context.Context) {
	if w == nil || w.svc == nil || w.svc.cfg == nil || !w.svc.cfg.QuotaAutoRepair {
		return
	}
	if w.svc.cfg.QuotaMode != "shadow" && w.svc.cfg.QuotaMode != "live" {
		return
	}
	pool := w.svc.GetPool()
	if pool == nil {
		return
	}

	w.observeShardQuorum(ctx)
	w.releaseDeadShardReservations(ctx)

	rows, err := w.loadActiveQuotas(ctx)
	if err != nil {
		slog.Error("quota repair: load active quotas failed", "error", err)
		return
	}

	for _, r := range rows {
		if int(r.shardID) >= len(w.svc.rdbs) {
			continue
		}
		if w.quorum != nil && w.quorum.DeadShardConfirmed(int(r.shardID)) {
			continue
		}
		rdb := w.svc.rdbs[r.shardID]
		redisExpected, quotaMissing, err := w.redisQuotaExpected(ctx, rdb, r.campaignID)
		if err != nil {
			continue
		}

		action, repairMicro, reason := decideQuotaRepair(r, redisExpected, quotaMissing)
		if action == "" || repairMicro <= 0 {
			continue
		}

		if err := w.enqueueQuotaRepair(ctx, r, action, redisExpected, repairMicro, reason); err != nil {
			slog.Error("quota repair: enqueue failed", "campaign_id", r.campaignID, "error", err)
			continue
		}
		metrics.QuotaRepairEnqueuedTotal.Inc()
	}
}

func (w *ReconWorker) loadActiveQuotas(ctx context.Context) ([]quotaRow, error) {
	p := w.svc.GetPool()
	rows, err := p.Query(ctx, `
		SELECT shard_id, campaign_id, reserved_amount, chunk_size, updated_at
		FROM campaign_quotas
		WHERE reserved_amount > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []quotaRow
	for rows.Next() {
		var r quotaRow
		var cid uuid.UUID
		if err := rows.Scan(&r.shardID, &cid, &r.reservedAmount, &r.chunkSize, &r.updatedAt); err != nil {
			return nil, err
		}
		r.campaignID = cid
		out = append(out, r)
	}
	return out, rows.Err()
}

func (w *ReconWorker) redisQuotaExpected(ctx context.Context, rdb redis.UniversalClient, campaignID uuid.UUID) (int64, bool, error) {
	cidStr := campaignID.String()
	quotaKey := "budget:quota:" + cidStr
	syncKey := "budget:sync:campaign:" + cidStr
	inflightKey := "budget:inflight:campaign:" + cidStr

	pipe := rdb.Pipeline()
	quotaCmd := pipe.Get(ctx, quotaKey)
	syncCmd := pipe.Get(ctx, syncKey)
	inflightCmd := pipe.Get(ctx, inflightKey)
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return 0, false, err
	}

	quotaVal, qErr := quotaCmd.Int64()
	syncVal, _ := syncCmd.Int64()
	inflightVal, _ := inflightCmd.Int64()
	quotaMissing := qErr == redis.Nil
	if quotaMissing {
		quotaVal = 0
	}
	return quotaVal + syncVal + inflightVal, quotaMissing, nil
}

func decideQuotaRepair(r quotaRow, redisExpected int64, quotaKeyMissing bool) (QuotaRepairAction, int64, string) {
	chunk := r.chunkSize
	if chunk <= 0 {
		chunk = 1
	}
	drift := r.reservedAmount - redisExpected
	absDrift := int64(math.Abs(float64(drift)))

	if quotaKeyMissing && r.reservedAmount > 0 &&
		time.Since(r.updatedAt) >= quotaCrashGapSeconds*time.Second {
		amount := r.reservedAmount
		if amount > chunk {
			amount = chunk
		}
		return QuotaRepairTopUpRedis, amount, "crash_gap_missing_redis_key"
	}

	if absDrift <= chunk {
		return "", 0, ""
	}

	if drift > 0 {
		amount := absDrift
		if amount > chunk*2 {
			amount = chunk
		}
		return QuotaRepairTopUpRedis, amount, "pg_reserved_exceeds_redis"
	}

	return QuotaRepairReleasePG, absDrift, "redis_exceeds_pg_reserved"
}

func (w *ReconWorker) enqueueQuotaRepair(
	ctx context.Context,
	r quotaRow,
	action QuotaRepairAction,
	redisExpected, repairMicro int64,
	reason string,
) error {
	payload, err := json.Marshal(QuotaRepairPayload{
		CampaignID:    r.campaignID.String(),
		ShardID:       r.shardID,
		Action:        string(action),
		PgReserved:    r.reservedAmount,
		RedisExpected: redisExpected,
		ChunkSize:     r.chunkSize,
		DriftMicro:    r.reservedAmount - redisExpected,
		RepairMicro:   repairMicro,
		Reason:        reason,
	})
	if err != nil {
		return err
	}
	q := db.New(w.svc.GetPool())
	ev, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: quotaRepairEventType,
		Payload:   payload,
	})
	if err != nil {
		return err
	}
	slog.Info("quota repair enqueued",
		"outbox_id", ev.ID,
		"campaign_id", r.campaignID,
		"action", action,
		"repair_micro", repairMicro,
		"reason", reason,
	)
	return nil
}

func (w *ReconWorker) observeShardQuorum(ctx context.Context) {
	if w.quorum == nil {
		return
	}
	for shardIdx, rdb := range w.svc.rdbs {
		w.quorum.ObserveShard(ctx, shardIdx, rdb)
	}
}

func (w *ReconWorker) releaseDeadShardReservations(ctx context.Context) {
	if w.quorum == nil || !w.svc.cfg.QuotaAutoRepair {
		return
	}
	pool := w.svc.GetPool()
	for shardIdx := range w.svc.rdbs {
		if !w.quorum.DeadShardConfirmed(shardIdx) {
			continue
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			continue
		}
		tag, err := tx.Exec(ctx, `
			UPDATE campaign_quotas
			SET reserved_amount = 0, updated_at = NOW()
			WHERE shard_id = $1 AND reserved_amount > 0`,
			int16(shardIdx))
		if err != nil {
			_ = tx.Rollback(ctx)
			slog.Error("dead shard quota release failed", "shard", shardIdx, "error", err)
			continue
		}
		if tag.RowsAffected() == 0 {
			_ = tx.Rollback(ctx)
			continue
		}
		metrics.QuotaDeadShardReleaseTotal.Add(float64(tag.RowsAffected()))
		adminID := uuid.MustParse(quotaRepairSystemAdmin)
		q := db.New(tx)
		w.svc.AuditLog(ctx, q, adminID, "QUOTA_DEAD_SHARD_RELEASE", "redis_shard",
			nil, map[string]any{
				"shard_id":      shardIdx,
				"rows_released": tag.RowsAffected(),
			}, map[string]any{"tx_source": "recon_worker"})
		if err := tx.Commit(ctx); err != nil {
			slog.Error("dead shard quota release commit failed", "shard", shardIdx, "error", err)
			continue
		}
		slog.Warn("released PG quota reservations for dead shard", "shard", shardIdx, "rows", tag.RowsAffected())
	}
}

func (w *ReconWorker) quorumDuration() time.Duration {
	return defaultDeadShardQuorum
}

// MonitorQuotaDrift logs drift beyond chunk_size (shadow metric path when auto-repair is off).
func (w *ReconWorker) MonitorQuotaDrift(ctx context.Context) {
	pool := w.svc.GetPool()
	if pool == nil {
		return
	}
	rows, err := w.loadActiveQuotas(ctx)
	if err != nil {
		return
	}
	for _, r := range rows {
		if int(r.shardID) >= len(w.svc.rdbs) {
			continue
		}
		rdb := w.svc.rdbs[r.shardID]
		pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			cancel()
			continue
		}
		cancel()

		redisExpected, _, err := w.redisQuotaExpected(ctx, rdb, r.campaignID)
		if err != nil {
			continue
		}
		drift := math.Abs(float64(r.reservedAmount - redisExpected))
		if drift > float64(r.chunkSize) {
			metrics.QuotaDriftDetectedTotal.Inc()
			slog.Error("QUOTA DRIFT DETECTED",
				"campaign_id", r.campaignID,
				"shard", r.shardID,
				"pg_reserved", r.reservedAmount,
				"redis_expected", redisExpected,
				"drift", drift,
				"chunk_size", r.chunkSize,
			)
		}
	}
}

// ApplyQuotaRepair executes a QUOTA_REPAIR outbox payload (PG is authority for release; Redis for top-up).
func (w *OutboxWorker) ApplyQuotaRepair(ctx context.Context, eventID int64, payload []byte) error {
	p, err := parseQuotaRepairPayload(payload)
	if err != nil {
		return err
	}
	campID, err := uuid.Parse(p.CampaignID)
	if err != nil {
		return fmt.Errorf("invalid campaign id: %w", err)
	}

	tx, err := w.svc.GetPool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	adminID := uuid.MustParse(quotaRepairSystemAdmin)
	auditMeta := map[string]any{
		"outbox_event_id": eventID,
		"reason":          p.Reason,
		"repair_micro":    p.RepairMicro,
	}

	switch QuotaRepairAction(p.Action) {
	case QuotaRepairTopUpRedis:
		if int(p.ShardID) >= len(w.svc.rdbs) {
			return fmt.Errorf("invalid shard_id %d", p.ShardID)
		}
		rdb := w.svc.rdbs[p.ShardID]
		quotaKey := fmt.Sprintf("budget:quota:%s", p.CampaignID)
		if err := rdb.IncrBy(ctx, quotaKey, p.RepairMicro).Err(); err != nil {
			return err
		}
		w.svc.AuditLog(ctx, db.New(tx), adminID, "QUOTA_REPAIR_TOPUP", quotaRepairTargetType,
			&campID, p, auditMeta)
	case QuotaRepairReleasePG:
		q := db.New(tx)
		if err := q.DecreaseCampaignQuotaReserved(ctx, db.DecreaseCampaignQuotaReservedParams{
			ShardID:        p.ShardID,
			CampaignID:     ads.ToUUID(campID),
			ReservedAmount: p.RepairMicro,
		}); err != nil {
			return err
		}
		w.svc.AuditLog(ctx, q, adminID, "QUOTA_REPAIR_RELEASE", quotaRepairTargetType,
			&campID, p, auditMeta)
	default:
		return fmt.Errorf("unknown quota repair action: %s", p.Action)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	metrics.QuotaRepairAppliedTotal.Inc()
	return nil
}

func parseQuotaRepairPayload(payload []byte) (QuotaRepairPayload, error) {
	var p QuotaRepairPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return p, err
	}
	if p.CampaignID == "" || p.RepairMicro <= 0 {
		return p, fmt.Errorf("invalid quota repair payload")
	}
	return p, nil
}

package management

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"time"

	"espx/internal/config"
	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const reconciliationAdjustEventType = "RECONCILIATION_ADJUST"

// BrokerPendingDeltaReader supplies unflushed broker budget deltas (M8-04); nil returns zero.
type BrokerPendingDeltaReader interface {
	PendingDeltaMicro(ctx context.Context, campaignID uuid.UUID) (int64, error)
}

// ReconciliationAdjustPayload is the outbox body for budget drift corrections (M3-04).
type ReconciliationAdjustPayload struct {
	RunID      int64  `json:"run_id,omitempty"`
	CampaignID string `json:"campaign_id"`
	CustomerID string `json:"customer_id"`
	ShardID    int16  `json:"shard_id"`
	LedgerAmt  int64  `json:"ledger_amount_micro"`
	RedisDelta int64  `json:"redis_delta_micro"`
	Reason     string `json:"reason"`
}

type campaignBudgetPG struct {
	campaignID    uuid.UUID
	customerID    uuid.UUID
	budgetLimit   int64
	currentSpend  int64
	quotaReserved int64
	updatedAt     time.Time
}

// ReconcileBudgetSnapshot scans dirty campaigns and checks the unified budget invariant (M3-01).
func (w *ReconWorker) ReconcileBudgetSnapshot(ctx context.Context) {
	if w == nil || w.svc == nil || w.svc.GetPool() == nil {
		return
	}
	w.observeShardQuorum(ctx)

	reconSvc := NewReconService(w.svc)
	campaignIDs, err := w.collectDirtyCampaignIDs(ctx)
	if err != nil {
		slog.Error("budget snapshot recon: dirty set scan failed", "error", err)
		return
	}

	var checked, skipped, discrepancies int
	for _, campID := range campaignIDs {
		ok, disc, skip := w.reconcileCampaignSnapshot(ctx, reconSvc, campID)
		if skip {
			skipped++
			continue
		}
		if ok {
			checked++
		}
		if disc {
			discrepancies++
		}
	}

	if discrepancies > 0 {
		metrics.ReconDiscrepanciesTotal.Add(float64(discrepancies))
	}
	slog.Debug("budget snapshot recon completed",
		"checked", checked,
		"skipped", skipped,
		"discrepancies", discrepancies,
	)
}

func (w *ReconWorker) collectDirtyCampaignIDs(ctx context.Context) ([]uuid.UUID, error) {
	seen := make(map[uuid.UUID]struct{})
	for shardIdx, rdb := range w.svc.rdbs {
		if w.quorum != nil && w.quorum.DeadShardConfirmed(shardIdx) {
			continue
		}
		var cursor uint64
		for {
			keys, next, err := rdb.SScan(ctx, "budget:dirty_campaigns", cursor, "", 200).Result()
			if err != nil {
				return nil, err
			}
			for _, idStr := range keys {
				id, err := uuid.Parse(idStr)
				if err != nil {
					continue
				}
				seen[id] = struct{}{}
			}
			if next == 0 {
				break
			}
			cursor = next
		}
	}
	out := make([]uuid.UUID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out, nil
}

func (w *ReconWorker) reconcileCampaignSnapshot(ctx context.Context, reconSvc *ReconService, campID uuid.UUID) (checked, discrepancy, skipped bool) {
	shardIdx := w.svc.sharder.GetShard(campID)
	if w.quorum != nil && w.quorum.DeadShardConfirmed(shardIdx) {
		return false, false, true
	}
	if shardIdx >= len(w.svc.rdbs) {
		return false, false, true
	}
	rdb := w.svc.rdbs[shardIdx]

	pg, err := w.loadCampaignBudgetPG(ctx, campID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, false, true
		}
		slog.Error("budget snapshot recon: load pg state failed", "campaign_id", campID, "error", err)
		return false, false, true
	}

	quotaMode := w.svc.cfg != nil && (w.svc.cfg.QuotaMode == "shadow" || w.svc.cfg.QuotaMode == "live")
	snap, err := ingestion.FetchBudgetReconSnapshot(ctx, rdb, campID, quotaMode)
	if err != nil {
		slog.Error("budget snapshot recon: redis snapshot failed", "campaign_id", campID, "error", err)
		return false, false, true
	}

	if snap.HasFence {
		return false, false, true
	}
	if snap.HasLock {
		return false, false, true
	}
	if w.shouldSkipSnapshotGrace(snap, pg.updatedAt) {
		return false, false, true
	}

	brokerPending := int64(0)
	if w.svc.brokerDeltas != nil {
		brokerPending, _ = w.svc.brokerDeltas.PendingDeltaMicro(ctx, campID)
	}

	pgRemaining := pg.budgetLimit - pg.currentSpend
	redisTotal := snap.RedisBudgetRemainingTotal(brokerPending)
	drift := pgRemaining - redisTotal
	tolerance := reconToleranceMicro(pg.budgetLimit)
	if abs(drift) <= tolerance {
		return true, false, false
	}

	metrics.ReconDriftMicro.WithLabelValues(campID.String()).Set(float64(abs(drift)))

	runID, err := reconSvc.createSnapshotRun(ctx)
	if err != nil {
		slog.Error("budget snapshot recon: create run failed", "error", err)
		return true, false, false
	}

	_, err = w.svc.GetPool().Exec(ctx, `
		INSERT INTO recon_discrepancies (run_id, campaign_id, customer_id, expected_spend, actual_spend, delta, redis_adjusted)
		VALUES ($1, $2, $3, $4, $5, $6, false)`,
		runID, ingestion.ToUUID(campID), pgtype.UUID{Bytes: pg.customerID, Valid: true},
		redisTotal, pgRemaining, drift,
	)
	if err != nil {
		slog.Error("budget snapshot recon: record discrepancy failed", "campaign_id", campID, "error", err)
		return true, false, false
	}

	chunk := reconSvc.autoAdjustChunkMicro()
	if abs(drift) > chunk {
		slog.Warn("budget snapshot drift exceeds auto-adjust chunk",
			"campaign_id", campID, "drift", drift, "chunk", chunk)
		return true, true, false
	}

	correction := drift
	if correction > chunk {
		correction = chunk
	} else if correction < -chunk {
		correction = -chunk
	}

	if err := w.enqueueReconciliationAdjust(ctx, runID, campID, pg.customerID, int16(shardIdx), -correction, correction, "budget_snapshot_invariant"); err != nil {
		slog.Error("budget snapshot recon: enqueue adjust failed", "campaign_id", campID, "error", err)
		metrics.ReconAdjustmentErrors.Inc()
		return true, true, false
	}
	metrics.ReconCorrectionsTotal.Inc()
	return true, true, false
}

func (w *ReconWorker) loadCampaignBudgetPG(ctx context.Context, campID uuid.UUID) (campaignBudgetPG, error) {
	var out campaignBudgetPG
	out.campaignID = campID
	err := w.svc.GetPool().QueryRow(ctx, `
		SELECT c.customer_id, c.budget_limit, c.current_spend, c.updated_at,
		       COALESCE(q.reserved_amount, 0)
		FROM campaigns c
		LEFT JOIN campaign_quotas q ON q.campaign_id = c.id
		WHERE c.id = $1`,
		ingestion.ToUUID(campID),
	).Scan(&out.customerID, &out.budgetLimit, &out.currentSpend, &out.updatedAt, &out.quotaReserved)
	return out, err
}

func (w *ReconWorker) shouldSkipSnapshotGrace(snap ingestion.BudgetReconSnapshot, lastPGUpdate time.Time) bool {
	if snap.Inflight <= 0 {
		return false
	}
	grace := reconGraceWindow(w.svc.cfg)
	return time.Since(lastPGUpdate) < grace
}

func reconGraceWindow(cfg *config.Config) time.Duration {
	if cfg == nil {
		return 15 * time.Second
	}
	ms := cfg.LedgerBatchFlushMs + cfg.BudgetSyncIntervalMs
	if ms <= 0 {
		return 15 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

func reconToleranceMicro(budgetLimit int64) int64 {
	pct := int64(math.Max(1, float64(budgetLimit)*0.0001))
	if pct < 1 {
		return 1
	}
	return pct
}

func (reconService *ReconService) createSnapshotRun(ctx context.Context) (int64, error) {
	now := time.Now().UTC()
	var id int64
	err := reconService.mgmt.GetPool().QueryRow(ctx, `
		INSERT INTO recon_runs (period_start, period_end, status) VALUES ($1, $2, 'SNAPSHOT') RETURNING id`,
		now, now,
	).Scan(&id)
	return id, err
}

func (w *ReconWorker) enqueueReconciliationAdjust(
	ctx context.Context,
	runID int64,
	campID, customerID uuid.UUID,
	shardID int16,
	ledgerAmt, redisDelta int64,
	reason string,
) error {
	payload, err := json.Marshal(ReconciliationAdjustPayload{
		RunID:      runID,
		CampaignID: campID.String(),
		CustomerID: customerID.String(),
		ShardID:    shardID,
		LedgerAmt:  ledgerAmt,
		RedisDelta: redisDelta,
		Reason:     reason,
	})
	if err != nil {
		return err
	}
	q := db.New(w.svc.GetPool())
	_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: reconciliationAdjustEventType,
		Payload:   payload,
	})
	return err
}

package management

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
)

// ReconWorker triggers periodic ledger-to-Redis reconciliation and quota reconciliation (Phase 1.5).
type ReconWorker struct {
	svc      *Service
	interval time.Duration
}

// NewReconWorker constructs a recon worker that runs on the given interval against the management service.
func NewReconWorker(svc *Service, interval time.Duration) *ReconWorker {
	return &ReconWorker{
		svc:      svc,
		interval: interval,
	}
}

// Start runs reconciliation for lagged hourly windows and campaign quotas until the context is cancelled.
func (w *ReconWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// Run quota reconciliation every 10 seconds
	quotaTicker := time.NewTicker(10 * time.Second)
	defer quotaTicker.Stop()

	reconSvc := NewReconService(w.svc)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			end := time.Now().Truncate(time.Hour).Add(-2 * time.Hour)
			start := end.Add(-time.Hour)
			if err := reconSvc.ReconcileWindow(ctx, start, end); err != nil {
				slog.Error("recon worker iteration failed", "error", err, "window", start)
			}
		case <-quotaTicker.C:
			if w.svc.cfg != nil && (w.svc.cfg.QuotaMode == "shadow" || w.svc.cfg.QuotaMode == "live") {
				w.ReconcileQuotas(ctx)
			}
		}
	}
}

// ReconcileQuotas checks for stuck reservations on dead shards and monitors quota drift (Phase 1.5.2 & 1.5.3).
func (w *ReconWorker) ReconcileQuotas(ctx context.Context) {
	pool := w.svc.GetPool()
	if pool == nil {
		return
	}

	// 1. Check shard health and handle dead shards (Phase 1.5.2)
	for shardIdx, rdb := range w.svc.rdbs {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := rdb.Ping(pingCtx).Err()
		cancel()

		if err != nil {
			slog.Error("redis shard is unreachable in recon, releasing stuck reservations", "shard", shardIdx, "error", err)
			// Shard is dead! Release all reservations on this shard that haven't been updated for 60 seconds
			_, dbErr := pool.Exec(ctx, `
				UPDATE campaign_quotas
				SET reserved_amount = 0,
					updated_at = NOW()
				WHERE shard_id = $1 AND updated_at < NOW() - INTERVAL '60 seconds' AND reserved_amount > 0
			`, shardIdx)
			if dbErr != nil {
				slog.Error("failed to release stuck reservations for dead shard", "shard", shardIdx, "error", dbErr)
			}
		}
	}

	// 2. Monitor quota drift (Phase 1.5.3)
	rows, err := pool.Query(ctx, `
		SELECT shard_id, campaign_id, reserved_amount, chunk_size
		FROM campaign_quotas
		WHERE reserved_amount > 0
	`)
	if err != nil {
		slog.Error("failed to query campaign quotas for drift monitoring", "error", err)
		return
	}
	defer rows.Close()

	type quotaRow struct {
		shardID        int16
		campaignID     uuid.UUID
		reservedAmount int64
		chunkSize      int64
	}

	var activeQuotas []quotaRow
	for rows.Next() {
		var r quotaRow
		var cid uuid.UUID
		if err := rows.Scan(&r.shardID, &cid, &r.reservedAmount, &r.chunkSize); err != nil {
			slog.Error("failed to scan quota row in recon", "error", err)
			continue
		}
		r.campaignID = cid
		activeQuotas = append(activeQuotas, r)
	}

	for _, r := range activeQuotas {
		if int(r.shardID) >= len(w.svc.rdbs) {
			continue
		}
		rdb := w.svc.rdbs[r.shardID]

		pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		pingErr := rdb.Ping(pingCtx).Err()
		cancel()
		if pingErr != nil {
			continue // skip drift check if shard is unreachable (handled by dead shard logic)
		}

		cidStr := r.campaignID.String()
		quotaKey := "budget:quota:" + cidStr
		syncKey := "budget:sync:campaign:" + cidStr
		inflightKey := "budget:inflight:campaign:" + cidStr

		pipe := rdb.Pipeline()
		quotaCmd := pipe.Get(ctx, quotaKey)
		syncCmd := pipe.Get(ctx, syncKey)
		inflightCmd := pipe.Get(ctx, inflightKey)
		_, _ = pipe.Exec(ctx)

		quotaVal, _ := quotaCmd.Int64()
		syncVal, _ := syncCmd.Int64()
		inflightVal, _ := inflightCmd.Int64()

		expectedReserved := quotaVal + syncVal + inflightVal
		drift := math.Abs(float64(r.reservedAmount - expectedReserved))

		if drift > float64(r.chunkSize) {
			slog.Error("QUOTA DRIFT DETECTED",
				"campaign_id", r.campaignID,
				"shard", r.shardID,
				"pg_reserved", r.reservedAmount,
				"redis_quota", quotaVal,
				"redis_sync", syncVal,
				"redis_inflight", inflightVal,
				"expected_reserved", expectedReserved,
				"drift", drift,
				"chunk_size", r.chunkSize,
			)
		}
	}
}

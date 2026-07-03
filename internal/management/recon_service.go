package management

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"espx/internal/ads"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	redis "github.com/redis/go-redis/v9"
)

// ReconService detects and corrects drift between Postgres ledger spend and Redis budget sync counters.
type ReconService struct {
	mgmt *Service
}

// NewReconService attaches reconciliation logic to the management service.
func NewReconService(svc *Service) *ReconService {
	return &ReconService{mgmt: svc}
}

// ReconcileWindow compares ledger totals to Redis sync keys for one time window and posts corrective entries.
func (s *ReconService) ReconcileWindow(ctx context.Context, start, end time.Time) error {
	opCtx, cancel := workerContext(ctx, workerBatchTimeout)
	defer cancel()

	run, err := s.createRun(opCtx, start, end)
	if err != nil {
		slog.Error("failed to create recon run record", "error", err, "start", start, "end", end)
		metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
		return err
	}

	ledgerRows, err := s.mgmt.GetPool().Query(opCtx, `
		SELECT campaign_id, COALESCE(SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END), 0)::bigint
		FROM balance_ledger
		WHERE created_at >= $1 AND created_at < $2
		  AND (type IN ('FEE', 'RECONCILIATION_ADJUST', 'REFUND'))
		GROUP BY campaign_id
	`, start, end)
	if err != nil {
		s.failRun(opCtx, run.ID, err)
		metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
		return err
	}
	defer ledgerRows.Close()

	ledgerMap := make(map[uuid.UUID]int64)
	for ledgerRows.Next() {
		var cid uuid.UUID
		var spent int64
		if err := ledgerRows.Scan(&cid, &spent); err != nil {
			slog.Error("failed to scan ledger row in recon run", "run_id", run.ID, "error", err)
			continue
		}
		ledgerMap[cid] = spent
	}

	discrepancies := 0
	var totalDelta int64

	for campID, ledgerSpent := range ledgerMap {
		syncKey := "budget:sync:campaign:" + campID.String()
		rdb := s.mgmt.getRDB(campID)
		if rdb == nil {
			slog.Error("no redis shard for campaign in recon", "campaign_id", campID)
			metrics.ReconAdjustmentErrors.Inc()
			continue
		}

		syncVal, err := rdb.Get(opCtx, syncKey).Int64()
		if err != nil && !errors.Is(err, redis.Nil) {
			slog.Error("failed to fetch campaign sync budget from Redis in recon", "campaign_id", campID, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			s.failRun(opCtx, run.ID, err)
			metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
			return err
		}

		delta := syncVal - ledgerSpent
		if delta == 0 {
			continue
		}

		var customerID pgtype.UUID
		err = s.mgmt.GetPool().QueryRow(opCtx, `SELECT customer_id FROM campaigns WHERE id = $1`, ads.ToUUID(campID)).Scan(&customerID)
		if err != nil {
			slog.Error("failed to resolve campaign customer in recon", "campaign_id", campID, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			continue
		}

		discrepancies++

		_, err = s.mgmt.GetPool().Exec(opCtx, `
			INSERT INTO recon_discrepancies (run_id, campaign_id, customer_id, expected_spend, actual_spend, delta, redis_adjusted)
			VALUES ($1, $2, $3, $4, $5, $6, false)
		`, run.ID, ads.ToUUID(campID), customerID, syncVal, ledgerSpent, delta)
		if err != nil {
			slog.Error("failed to record recon discrepancy to postgres", "run_id", run.ID, "campaign_id", campID, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			s.failRun(opCtx, run.ID, err)
			metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
			return err
		}

		adjType := "RECONCILIATION_ADJUST"
		_, err = s.mgmt.GetPool().Exec(opCtx, `
			INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, created_at)
			VALUES ($1, $2, $3, $4, NOW())
		`, customerID, ads.ToUUID(campID), -delta, adjType)
		if err != nil {
			slog.Error("failed to insert corrective ledger entry for recon", "campaign_id", campID, "delta", delta, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			continue
		}

		if err := s.adjustRedisBudgetAtomically(opCtx, rdb, campID, delta); err != nil {
			slog.Error("failed to adjust Redis budget atomically in recon", "campaign_id", campID, "delta", delta, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
			continue
		}

		_, err = s.mgmt.GetPool().Exec(opCtx, `
			UPDATE recon_discrepancies 
			SET redis_adjusted = true 
			WHERE run_id = $1 AND campaign_id = $2
		`, run.ID, ads.ToUUID(campID))
		if err != nil {
			slog.Error("failed to mark recon discrepancy as adjusted in postgres", "run_id", run.ID, "campaign_id", campID, "error", err)
			metrics.ReconAdjustmentErrors.Inc()
		}

		totalDelta += delta
	}

	_, err = s.mgmt.GetPool().Exec(opCtx, `
		UPDATE recon_runs 
		SET status = 'COMPLETED', total_delta = $1, campaigns_checked = $2, discrepancies_found = $3, completed_at = NOW()
		WHERE id = $4
	`, totalDelta, len(ledgerMap), discrepancies, run.ID)
	if err != nil {
		slog.Error("failed to finalize recon run in postgres", "run_id", run.ID, "error", err)
		metrics.ReconRunsTotal.WithLabelValues("failed").Inc()
		return err
	}

	metrics.ReconRunsTotal.WithLabelValues("success").Inc()
	if discrepancies > 0 {
		metrics.ReconDiscrepanciesTotal.Add(float64(discrepancies))
	}
	metrics.ReconTotalDelta.Add(float64(abs(totalDelta)))

	slog.Info("reconciliation completed",
		"period", start.Format(time.RFC3339)+"-"+end.Format(time.RFC3339),
		"delta", totalDelta,
		"discrepancies", discrepancies,
	)
	return nil
}

// adjustRedisBudgetAtomically applies a recon delta in Redis without leaving stale zero or negative budget keys.
func (s *ReconService) adjustRedisBudgetAtomically(ctx context.Context, rdb redis.UniversalClient, campID uuid.UUID, delta int64) error {
	script := `
		local key = KEYS[1]
		local delta = tonumber(ARGV[1])
		local newVal = redis.call("INCRBY", key, delta)
		if newVal <= 0 then
			redis.call("DEL", key)
			return 0
		end
		return newVal
	`
	_, err := rdb.Eval(ctx, script, []string{"budget:sync:campaign:" + campID.String()}, delta).Result()
	return err
}

// createRun records a recon run row so partial failures and outcomes remain auditable.
func (s *ReconService) createRun(ctx context.Context, start, end time.Time) (struct{ ID int64 }, error) {
	var run struct{ ID int64 }
	err := s.mgmt.GetPool().QueryRow(ctx, `
		INSERT INTO recon_runs (period_start, period_end, status) VALUES ($1, $2, 'PENDING') RETURNING id
	`, start, end).Scan(&run.ID)
	return run, err
}

// failRun marks a recon run as failed when the pipeline cannot complete the window.
func (s *ReconService) failRun(ctx context.Context, id int64, err error) {
	_, execErr := s.mgmt.GetPool().Exec(ctx, `UPDATE recon_runs SET status = 'FAILED' WHERE id = $1`, id)
	if execErr != nil {
		slog.Error("failed to mark recon run status as failed in postgres", "run_id", id, "error", execErr)
	}
	slog.Error("reconciliation run failed", "run_id", id, "error", err)
}

// abs returns the absolute value of delta for recon metrics that must report magnitude only.
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

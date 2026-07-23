package management

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"espx/internal/config"
)

// ReconWorker triggers periodic ledger-to-Redis reconciliation and quota reconciliation (Phase 1.5).
type ReconWorker struct {
	svc      *Service
	interval time.Duration
	quorum   *ShardQuorumTracker
}

// NewReconWorker constructs a recon worker that runs on the given interval against the management service.
func NewReconWorker(svc *Service, interval time.Duration) *ReconWorker {
	numShards := 1
	if svc != nil {
		numShards = len(svc.rdbs)
	}
	return &ReconWorker{
		svc:      svc,
		interval: interval,
		quorum:   NewShardQuorumTracker(numShards, defaultDeadShardQuorum),
	}
}

// NewReconWorkerWithQuorum is for tests that need a shorter dead-shard confirmation window.
func NewReconWorkerWithQuorum(svc *Service, interval, quorum time.Duration) *ReconWorker {
	w := NewReconWorker(svc, interval)
	if w.quorum != nil {
		w.quorum = NewShardQuorumTracker(len(svc.rdbs), quorum)
	}
	return w
}

// Quorum exposes the shard health tracker for chaos tests.
func (w *ReconWorker) Quorum() *ShardQuorumTracker {
	if w == nil {
		return nil
	}
	return w.quorum
}

// Start runs reconciliation for lagged hourly windows and campaign quotas until the context is cancelled.
func (w *ReconWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	quotaTicker := time.NewTicker(10 * time.Second)
	defer quotaTicker.Stop()

	drainCheckTicker := time.NewTicker(time.Minute)
	defer drainCheckTicker.Stop()

	snapshotTicker := time.NewTicker(reconSnapshotInterval(w.svc.cfg))
	defer snapshotTicker.Stop()

	reconSvc := NewReconService(w.svc)

	for {
		select {
		case <-ctx.Done():
			return
		case <-snapshotTicker.C:
			if err := w.svc.withPgLow(ctx, func(runCtx context.Context) error {
				w.ReconcileBudgetSnapshot(runCtx)
				return nil
			}); err != nil && !errors.Is(err, ErrMgmtPgGateRejected) {
				slog.Error("budget snapshot recon failed", "error", err)
			}
		case <-ticker.C:
			end := time.Now().Truncate(time.Hour).Add(-2 * time.Hour)
			start := end.Add(-time.Hour)
			if err := w.svc.withPgLow(ctx, func(runCtx context.Context) error {
				return reconSvc.ReconcileWindow(runCtx, start, end)
			}); err != nil && !errors.Is(err, ErrMgmtPgGateRejected) {
				slog.Error("recon worker iteration failed", "error", err, "window", start)
			}
		case <-quotaTicker.C:
			if w.svc.cfg != nil && (w.svc.cfg.QuotaMode == "shadow" || w.svc.cfg.QuotaMode == "live") {
				if err := w.svc.withPgLow(ctx, func(runCtx context.Context) error {
					w.ReconcileQuotas(runCtx)
					return nil
				}); err != nil && !errors.Is(err, ErrMgmtPgGateRejected) {
					slog.Error("quota recon failed", "error", err)
				}
			}
		case <-drainCheckTicker.C:
			w.svc.CheckStuckDrainJobs(ctx)
			reconSvc.AlertStaleUnresolvedDiscrepancies(ctx)
		}
	}
}

// ReconcileQuotas checks shard quorum, repairs drift, and monitors quota health (M3).
func (w *ReconWorker) ReconcileQuotas(ctx context.Context) {
	if w.svc == nil {
		return
	}
	w.observeShardQuorum(ctx)
	if w.svc.cfg != nil && w.svc.cfg.QuotaAutoRepair {
		w.RepairQuotaDrift(ctx)
	} else {
		w.MonitorQuotaDrift(ctx)
	}
}

func reconSnapshotInterval(cfg *config.Config) time.Duration {
	if cfg == nil {
		return 30 * time.Second
	}
	if cfg.Management.ReconSnapshotIntervalMs > 0 {
		return time.Duration(cfg.Management.ReconSnapshotIntervalMs) * time.Millisecond
	}
	ms := cfg.BudgetSyncIntervalMs
	if ms <= 0 {
		ms = 5000
	}
	return time.Duration(ms) * time.Millisecond
}

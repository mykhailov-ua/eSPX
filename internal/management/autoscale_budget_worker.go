package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ads"
)

// AutoscaleBudgetWorker periodically shifts budget from low-CTR to high-CTR campaigns per customer.
type AutoscaleBudgetWorker struct {
	svc         *Service
	syncWorkers []*ads.SyncWorker
}

// NewAutoscaleBudgetWorker binds budget autoscaling to the service and budget sync workers.
func NewAutoscaleBudgetWorker(svc *Service, syncWorkers []*ads.SyncWorker) *AutoscaleBudgetWorker {
	return &AutoscaleBudgetWorker{
		svc:         svc,
		syncWorkers: syncWorkers,
	}
}

// Start runs budget autoscaling on a fixed interval until the context is cancelled.
func (w *AutoscaleBudgetWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.svc.AutoscaleBudgets(ctx, w.syncWorkers); err != nil {
				slog.Error("autoscale budgets run failed", "error", err)
			}
		}
	}
}

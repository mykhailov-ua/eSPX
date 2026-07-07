package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ads"
)

// DeliveryOptimizerWorker runs periodic delivery tuning ticks (bid floor today; autoscale/pacing in M5.0).
type DeliveryOptimizerWorker struct {
	svc         *Service
	syncWorkers []*ads.SyncWorker
}

// NewDeliveryOptimizerWorker binds the unified delivery optimizer to budget sync workers.
func NewDeliveryOptimizerWorker(svc *Service, syncWorkers []*ads.SyncWorker) *DeliveryOptimizerWorker {
	return &DeliveryOptimizerWorker{
		svc:         svc,
		syncWorkers: syncWorkers,
	}
}

// Start runs the optimizer on a fixed interval until the context is cancelled.
func (w *DeliveryOptimizerWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick(ctx)
		}
	}
}

func (w *DeliveryOptimizerWorker) tick(ctx context.Context) {
	for _, sw := range w.syncWorkers {
		if sw != nil {
			sw.SyncAll(ctx)
		}
	}
	if _, err := w.svc.OptimizeBidFloors(ctx); err != nil {
		slog.Error("bid floor optimizer failed", "error", err)
	}
}

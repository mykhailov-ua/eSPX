package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ingestion"
)

// DeliveryOptimizerWorker runs the unified M5.0 delivery pass (pacing, autoscale, MAB, bid floors).
type DeliveryOptimizerWorker struct {
	svc         *Service
	syncWorkers []*ingestion.SyncWorker
	lastMABRun  time.Time
}

// NewDeliveryOptimizerWorker binds the unified delivery optimizer to budget sync workers.
func NewDeliveryOptimizerWorker(svc *Service, syncWorkers []*ingestion.SyncWorker) *DeliveryOptimizerWorker {
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
	runMAB := false
	mabInterval := time.Duration(w.svc.cfg.MABIntervalMs) * time.Millisecond
	if mabInterval <= 0 {
		mabInterval = 15 * time.Minute
	}
	now := time.Now()
	if w.lastMABRun.IsZero() || now.Sub(w.lastMABRun) >= mabInterval {
		runMAB = true
		w.lastMABRun = now
	}

	if err := w.svc.RunDeliveryOptimizerTick(ctx, w.syncWorkers, runMAB); err != nil {
		slog.Error("delivery optimizer tick failed", "error", err, "run_mab", runMAB)
	}
}

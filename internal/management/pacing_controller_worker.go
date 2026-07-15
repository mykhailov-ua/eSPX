package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ingestion"
)

// PacingControllerWorker periodically rebalances campaign pacing modes based on actual spend versus the daily curve.
type PacingControllerWorker struct {
	svc         *Service
	syncWorkers []*ingestion.SyncWorker
}

// NewPacingControllerWorker binds the closed-loop pacing controller to the service and budget sync workers.
func NewPacingControllerWorker(svc *Service, syncWorkers []*ingestion.SyncWorker) *PacingControllerWorker {
	return &PacingControllerWorker{
		svc:         svc,
		syncWorkers: syncWorkers,
	}
}

// Start runs the pacing controller on a fixed interval until the context is cancelled.
func (w *PacingControllerWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.svc.ClosedLoopPacingController(ctx, w.syncWorkers); err != nil {
				slog.Error("closed-loop pacing controller run failed", "error", err)
			}
		}
	}
}

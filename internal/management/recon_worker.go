package management

import (
	"context"
	"log/slog"
	"time"
)

// ReconWorker triggers periodic ledger-to-Redis reconciliation for completed hourly billing windows.
type ReconWorker struct {
	svc      *ReconService
	interval time.Duration
}

// NewReconWorker constructs a recon worker that runs on the given interval against the management service.
func NewReconWorker(svc *Service, interval time.Duration) *ReconWorker {
	return &ReconWorker{
		svc:      NewReconService(svc),
		interval: interval,
	}
}

// Start runs reconciliation for lagged hourly windows until the context is cancelled.
func (w *ReconWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			end := time.Now().Truncate(time.Hour).Add(-2 * time.Hour)
			start := end.Add(-time.Hour)
			if err := w.svc.ReconcileWindow(ctx, start, end); err != nil {
				slog.Error("recon worker iteration failed", "error", err, "window", start)
			}
		}
	}
}

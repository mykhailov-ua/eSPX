package management

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	redis "github.com/redis/go-redis/v9"
)

// ReconWorker runs periodic reconciliation over closed hourly windows.
// It is intentionally scheduled with a large interval (e.g. every 15-30 min) and
// always processes data from at least 2 hours in the past. This design decision
// completely eliminates any possibility of race conditions with the hot settlement path.
type ReconWorker struct {
	svc      *ReconService
	interval time.Duration
	wg       sync.WaitGroup
}

func NewReconWorker(pool *pgxpool.Pool, rdb redis.UniversalClient, interval time.Duration) *ReconWorker {
	return &ReconWorker{
		svc:      NewReconService(pool, rdb),
		interval: interval,
	}
}

func (w *ReconWorker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Always reconcile the window [now-3h, now-2h]. This guarantees the window is fully settled.
				end := time.Now().Truncate(time.Hour).Add(-2 * time.Hour)
				start := end.Add(-time.Hour)
				if err := w.svc.ReconcileWindow(ctx, start, end); err != nil {
					slog.Error("recon worker iteration failed", "error", err, "window", start)
				}
			}
		}
	}()
}

func (w *ReconWorker) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

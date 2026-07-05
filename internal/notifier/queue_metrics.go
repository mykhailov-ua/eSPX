package notifier

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartQueueMetricsScraper periodically updates queue depth and oldest-age gauges.
func StartQueueMetricsScraper(ctx context.Context, pool *pgxpool.Pool, interval time.Duration) {
	if pool == nil {
		return
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}

	scrape := func() {
		scrapeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var pending int64
		var oldestPendingSeconds float64
		var processing int64
		var oldestProcessingSeconds float64
		err := pool.QueryRow(scrapeCtx, `
			SELECT COUNT(*) FILTER (WHERE status = 'PENDING')::bigint,
			       COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status = 'PENDING'))), 0)::float8,
			       COUNT(*) FILTER (WHERE status = 'PROCESSING')::bigint,
			       COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(claimed_at) FILTER (WHERE status = 'PROCESSING'))), 0)::float8
			FROM notifier.notifications
			WHERE status IN ('PENDING', 'PROCESSING')`).Scan(
			&pending, &oldestPendingSeconds, &processing, &oldestProcessingSeconds,
		)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("notifier queue metrics scrape failed", "error", err)
			}
			return
		}
		queuePendingTotal.Set(float64(pending))
		queueOldestPendingSeconds.Set(oldestPendingSeconds)
		queueProcessingTotal.Set(float64(processing))
		queueOldestProcessingSeconds.Set(oldestProcessingSeconds)
	}

	scrape()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scrape()
		}
	}
}

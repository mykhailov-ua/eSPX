package notifier

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RetentionJanitor deletes old SENT and FAILED notification rows.
type RetentionJanitor struct {
	pool            *pgxpool.Pool
	interval        time.Duration
	sentRetention   time.Duration
	failedRetention time.Duration
}

// NewRetentionJanitor constructs a janitor with positive day counts defaulting to 30/90.
func NewRetentionJanitor(pool *pgxpool.Pool, interval time.Duration, sentDays, failedDays int) *RetentionJanitor {
	if interval <= 0 {
		interval = time.Hour
	}
	if sentDays <= 0 {
		sentDays = 30
	}
	if failedDays <= 0 {
		failedDays = 7
	}
	return &RetentionJanitor{
		pool:            pool,
		interval:        interval,
		sentRetention:   time.Duration(sentDays) * 24 * time.Hour,
		failedRetention: time.Duration(failedDays) * 24 * time.Hour,
	}
}

// Start runs periodic retention scans until ctx is cancelled.
func (j *RetentionJanitor) Start(ctx context.Context) {
	if j == nil || j.pool == nil {
		return
	}

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	j.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.runOnce(ctx)
		}
	}
}

func (j *RetentionJanitor) runOnce(ctx context.Context) {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	sentDeleted, err := j.deleteOlderThan(opCtx, "SENT", j.sentRetention)
	if err != nil {
		slog.Warn("notifier retention janitor sent purge failed", "error", err)
		return
	}
	failedDeleted, err := j.deleteOlderThan(opCtx, "FAILED", j.failedRetention)
	if err != nil {
		slog.Warn("notifier retention janitor failed purge failed", "error", err)
		return
	}

	if sentDeleted > 0 || failedDeleted > 0 {
		slog.Info("notifier retention janitor cycle complete",
			"sent_deleted", sentDeleted,
			"failed_deleted", failedDeleted,
		)
	}
}

func (j *RetentionJanitor) deleteOlderThan(ctx context.Context, status string, age time.Duration) (int64, error) {
	tag, err := j.pool.Exec(ctx, `
		DELETE FROM notifier.notifications
		WHERE status = $1
		  AND created_at < NOW() - ($2::bigint * interval '1 second')`,
		status, int64(age.Seconds()),
	)
	if err != nil {
		return 0, err
	}
	deleted := tag.RowsAffected()
	if deleted > 0 {
		retentionDeletedTotal.WithLabelValues(status).Add(float64(deleted))
	}
	return deleted, nil
}

package management

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UsageDailyFlushWorker copies today's usage_meters delta into billing.usage_daily.
type UsageDailyFlushWorker struct {
	pool     *pgxpool.Pool
	interval time.Duration
}

// NewUsageDailyFlushWorker constructs a daily usage flush worker.
func NewUsageDailyFlushWorker(pool *pgxpool.Pool, interval time.Duration) *UsageDailyFlushWorker {
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	return &UsageDailyFlushWorker{pool: pool, interval: interval}
}

// Start runs until ctx is cancelled.
func (w *UsageDailyFlushWorker) Start(ctx context.Context) {
	if w == nil || w.pool == nil {
		return
	}
	slog.Info("usage daily flush worker starting", "interval", w.interval)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.Flush(ctx, time.Now().UTC()); err != nil {
				slog.Error("usage daily flush failed", "error", err)
			}
		}
	}
}

// Flush copies monthly meter totals into usage_daily for the current UTC date.
func (w *UsageDailyFlushWorker) Flush(ctx context.Context, now time.Time) error {
	usageDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	period := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	rows, err := w.pool.Query(ctx, `
		SELECT customer_id, meter, value
		FROM billing.usage_meters
		WHERE period = $1`, period)
	if err != nil {
		return err
	}
	defer rows.Close()

	var flushed int
	for rows.Next() {
		var custID uuid.UUID
		var meter string
		var value int64
		if err := rows.Scan(&custID, &meter, &value); err != nil {
			return err
		}
		_, err := w.pool.Exec(ctx, `
			INSERT INTO billing.usage_daily (customer_id, usage_date, meter, value)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (customer_id, usage_date, meter) DO UPDATE
			SET value = EXCLUDED.value`,
			custID, usageDate, meter, value)
		if err != nil {
			return err
		}
		flushed++
	}
	if flushed > 0 {
		slog.Info("usage daily flush complete", "date", usageDate.Format("2006-01-02"), "rows", flushed)
	}
	return rows.Err()
}

package management

import (
	"context"
	"strings"

	"espx/internal/metrics"
)

// recordOutboxLagMetrics updates Prometheus gauges from the pending outbox queue depth and age.
func (w *OutboxWorker) recordOutboxLagMetrics(ctx context.Context) {
	if w.svc == nil || w.svc.GetPool() == nil {
		return
	}
	opCtx, cancel := workerContext(ctx, workerOutboxTimeout)
	defer cancel()

	var pending int64
	var oldestSeconds float64
	err := w.svc.GetPool().QueryRow(opCtx, `
		SELECT COUNT(*)::bigint,
		       COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0)::float8
		FROM outbox_events
		WHERE status = 'PENDING'`).Scan(&pending, &oldestSeconds)
	if err != nil {
		if ctx.Err() != nil || strings.Contains(err.Error(), "closed pool") {
			return
		}
		return
	}
	metrics.ManagementOutboxPendingTotal.Set(float64(pending))
	metrics.ManagementOutboxOldestPendingSeconds.Set(oldestSeconds)
}

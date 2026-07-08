package management

import (
	"context"

	"espx/internal/database"
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
		if ctx.Err() != nil || database.IsShutdownError(err) {
			return
		}
		return
	}
	metrics.ManagementOutboxPendingTotal.Set(float64(pending))
	metrics.ManagementOutboxOldestPendingSeconds.Set(oldestSeconds)

	if w.svc != nil && w.svc.alerter != nil && pending > 0 {
		threshold := float64(w.svc.alerter.OutboxStuckThresholdSec())
		if oldestSeconds >= threshold {
			w.svc.alerter.AlertOutboxStuck(pending, oldestSeconds)
		}
	}
}

func (w *OutboxWorker) recordOutboxLagFromValues(pending int64, oldestSeconds float64) {
	metrics.ManagementOutboxPendingTotal.Set(float64(pending))
	metrics.ManagementOutboxOldestPendingSeconds.Set(oldestSeconds)
	if w.svc != nil && w.svc.alerter != nil && pending > 0 {
		threshold := float64(w.svc.alerter.OutboxStuckThresholdSec())
		if oldestSeconds >= threshold {
			w.svc.alerter.AlertOutboxStuck(pending, oldestSeconds)
		}
	}
}

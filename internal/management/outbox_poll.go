package management

import (
	"time"

	"espx/internal/metrics"
)

const (
	outboxPollActiveInterval = 20 * time.Millisecond
	outboxPollIdleMax        = 250 * time.Millisecond
)

// outboxPollBackoff implements coefficient backoff for idle outbox polling (M-DB-PG-3).
type outboxPollBackoff struct {
	idle time.Duration
}

func newOutboxPollBackoff() *outboxPollBackoff {
	return &outboxPollBackoff{idle: outboxPollActiveInterval}
}

func (b *outboxPollBackoff) next(processed int) time.Duration {
	if processed > 0 {
		b.idle = outboxPollActiveInterval
		metrics.OutboxPollIntervalMs.Observe(float64(outboxPollActiveInterval) / float64(time.Millisecond))
		return 0
	}
	if b.idle < outboxPollActiveInterval {
		b.idle = outboxPollActiveInterval
	}
	next := b.idle * 2
	if next > outboxPollIdleMax {
		next = outboxPollIdleMax
	}
	b.idle = next
	metrics.OutboxPollIntervalMs.Observe(float64(next) / float64(time.Millisecond))
	return next
}

package notifier

import (
	"sync"
	"time"
)

// recipientRateLimiter caps enqueue bursts per recipient using a sliding minute window.
type recipientRateLimiter struct {
	mu             sync.Mutex
	limitPerMinute int
	windows        map[string][]time.Time
}

func newRecipientRateLimiter(limitPerMinute int) *recipientRateLimiter {
	if limitPerMinute <= 0 {
		return nil
	}
	return &recipientRateLimiter{
		limitPerMinute: limitPerMinute,
		windows:        make(map[string][]time.Time),
	}
}

func (limiter *recipientRateLimiter) allow(recipient string) bool {
	if limiter == nil || recipient == "" {
		return true
	}

	now := time.Now()
	cutoff := now.Add(-time.Minute)

	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	times := limiter.windows[recipient]
	filtered := times[:0]
	for _, ts := range times {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}
	if len(filtered) >= limiter.limitPerMinute {
		limiter.windows[recipient] = filtered
		return false
	}
	filtered = append(filtered, now)
	limiter.windows[recipient] = filtered
	return true
}

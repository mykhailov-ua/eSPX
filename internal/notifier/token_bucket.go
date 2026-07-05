package notifier

import (
	"sync"
	"time"
)

// tokenBucket limits sustained request rate with optional burst capacity.
type tokenBucket struct {
	mu           sync.Mutex
	tokens       float64
	maxTokens    float64
	refillPerSec float64
	lastRefill   time.Time
	blockedUntil time.Time
}

func newTokenBucket(perMinute int) *tokenBucket {
	if perMinute <= 0 {
		return nil
	}
	rate := float64(perMinute) / 60.0
	return &tokenBucket{
		tokens:       float64(perMinute),
		maxTokens:    float64(perMinute),
		refillPerSec: rate,
		lastRefill:   time.Now(),
	}
}

func (bucket *tokenBucket) allow(now time.Time) bool {
	if bucket == nil {
		return true
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	if now.Before(bucket.blockedUntil) {
		return false
	}

	elapsed := now.Sub(bucket.lastRefill).Seconds()
	if elapsed > 0 {
		bucket.tokens += elapsed * bucket.refillPerSec
		if bucket.tokens > bucket.maxTokens {
			bucket.tokens = bucket.maxTokens
		}
		bucket.lastRefill = now
	}

	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

func (bucket *tokenBucket) backoff(d time.Duration) {
	if bucket == nil || d <= 0 {
		return
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	until := time.Now().Add(d)
	if until.After(bucket.blockedUntil) {
		bucket.blockedUntil = until
	}
	bucket.tokens = 0
}

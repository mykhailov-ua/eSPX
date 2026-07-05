package notifier

import (
	"sync"
	"time"
)

// recipientRateLimiter caps enqueue bursts per recipient using a token bucket.
type recipientRateLimiter struct {
	mu      sync.Mutex
	limit   int
	buckets map[string]*tokenBucket
}

func newRecipientRateLimiter(limitPerMinute int) *recipientRateLimiter {
	if limitPerMinute <= 0 {
		return nil
	}
	return &recipientRateLimiter{
		limit:   limitPerMinute,
		buckets: make(map[string]*tokenBucket),
	}
}

func (limiter *recipientRateLimiter) allow(recipient string) bool {
	if limiter == nil || recipient == "" {
		return true
	}

	now := time.Now()
	limiter.mu.Lock()
	bucket, ok := limiter.buckets[recipient]
	if !ok {
		bucket = newTokenBucket(limiter.limit)
		limiter.buckets[recipient] = bucket
	}
	limiter.mu.Unlock()
	return bucket.allow(now)
}

// providerRateLimiter caps delivery per provider and recipient using independent token buckets.
type providerRateLimiter struct {
	mu      sync.Mutex
	limits  map[string]int // provider name -> per-minute limit; missing or 0 = unlimited
	buckets map[string]*tokenBucket
}

func newProviderRateLimiter(limits map[string]int) *providerRateLimiter {
	if len(limits) == 0 {
		return nil
	}
	filtered := make(map[string]int, len(limits))
	for provider, limit := range limits {
		if limit > 0 {
			filtered[provider] = limit
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return &providerRateLimiter{
		limits:  filtered,
		buckets: make(map[string]*tokenBucket),
	}
}

func providerRecipientKey(provider, recipient string) string {
	if recipient == "" {
		recipient = "_default"
	}
	return provider + ":" + recipient
}

func (limiter *providerRateLimiter) Allow(provider, recipient string) bool {
	if limiter == nil {
		return true
	}
	limit, ok := limiter.limits[provider]
	if !ok || limit <= 0 {
		return true
	}

	key := providerRecipientKey(provider, recipient)
	now := time.Now()

	limiter.mu.Lock()
	bucket, ok := limiter.buckets[key]
	if !ok {
		bucket = newTokenBucket(limit)
		limiter.buckets[key] = bucket
	}
	limiter.mu.Unlock()
	return bucket.allow(now)
}

func (limiter *providerRateLimiter) Backoff(provider, recipient string, d time.Duration) {
	if limiter == nil || d <= 0 {
		return
	}
	if _, ok := limiter.limits[provider]; !ok {
		return
	}

	key := providerRecipientKey(provider, recipient)
	limiter.mu.Lock()
	bucket, ok := limiter.buckets[key]
	if !ok {
		bucket = newTokenBucket(limiter.limits[provider])
		limiter.buckets[key] = bucket
	}
	limiter.mu.Unlock()
	bucket.backoff(d)
}

func deliveryRateLimitsFromOptions(opts ServiceOptions) map[string]int {
	limits := make(map[string]int)
	if opts.TelegramRateLimitPerMinute > 0 {
		limits["TELEGRAM"] = opts.TelegramRateLimitPerMinute
	}
	if opts.RateLimitPerMinute > 0 {
		for _, provider := range []string{"SLACK", "SMS", "SMTP"} {
			if _, set := limits[provider]; !set {
				limits[provider] = opts.RateLimitPerMinute
			}
		}
	}
	return limits
}

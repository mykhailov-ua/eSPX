package notifier

import (
	"time"

	"espx/internal/config"
)

// ServiceOptions tunes queue delivery, deduplication, and rate limiting.
type ServiceOptions struct {
	DedupCooldownSec   int64
	ClaimStaleSec      int64
	GroupParallelism   int
	RateLimitPerMinute int
}

func defaultServiceOptions() ServiceOptions {
	return ServiceOptions{
		DedupCooldownSec:   300,
		ClaimStaleSec:      300,
		GroupParallelism:   2,
		RateLimitPerMinute: 60,
	}
}

func (opts ServiceOptions) dedupCooldown() time.Duration {
	if opts.DedupCooldownSec <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(opts.DedupCooldownSec) * time.Second
}

func (opts ServiceOptions) claimStale() time.Duration {
	if opts.ClaimStaleSec <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(opts.ClaimStaleSec) * time.Second
}

func (opts ServiceOptions) groupParallelism() int {
	if opts.GroupParallelism <= 0 {
		return 1
	}
	return opts.GroupParallelism
}

// ServiceOptionsFromConfig maps notifier tuning from startup config.
func ServiceOptionsFromConfig(cfg *config.Config) ServiceOptions {
	if cfg == nil {
		return defaultServiceOptions()
	}
	n := cfg.Notifier
	return ServiceOptions{
		DedupCooldownSec:   int64(n.DedupCooldownSec),
		ClaimStaleSec:      int64(n.ClaimStaleSec),
		GroupParallelism:   n.GroupParallelism,
		RateLimitPerMinute: n.RateLimitPerMinute,
	}
}

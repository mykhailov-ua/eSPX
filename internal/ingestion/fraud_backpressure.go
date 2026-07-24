package ingestion

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"espx/internal/metrics"

	redis "github.com/redis/go-redis/v9"
)

const fraudAggForceKey = "fraud:agg_force"

// FraudBackpressureConfig tunes M14-11/M14-12 fraud consumer lag → force aggregate.
type FraudBackpressureConfig struct {
	Rdbs        []redis.UniversalClient
	Writer      *FraudStreamWriter
	Stream      string // tracker fraud stream name
	EventStream string // ad:events:stream for PEL age (GAP-OPS-04)
	Group       string
	LagSec      int
	Interval    time.Duration
}

// StartFraudBackpressureWatcher polls Redis for force-agg flag and publishes PEL age gauges.
func StartFraudBackpressureWatcher(ctx context.Context, cfg FraudBackpressureConfig) {
	if cfg.Writer == nil || len(cfg.Rdbs) == 0 {
		return
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 2 * time.Second
	}
	if cfg.LagSec <= 0 {
		cfg.LagSec = 30
	}
	go func() {
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				force := readFraudAggForce(ctx, cfg.Rdbs)
				cfg.Writer.SetForceAggregate(force)
				publishStreamPELAges(ctx, cfg)
			}
		}
	}()
}

func readFraudAggForce(ctx context.Context, rdbs []redis.UniversalClient) bool {
	for _, rdb := range rdbs {
		if rdb == nil {
			continue
		}
		v, err := rdb.Get(ctx, fraudAggForceKey).Result()
		if err != nil {
			continue
		}
		if v == "1" || v == "true" {
			return true
		}
	}
	return false
}

// PublishFraudConsumerLag sets fraud:agg_force when consumer idle age exceeds threshold (processor).
func PublishFraudConsumerLag(ctx context.Context, rdb redis.UniversalClient, stream, group string, lagSec int) {
	if rdb == nil || stream == "" || group == "" || lagSec <= 0 {
		return
	}
	age := oldestPELIdleSeconds(ctx, rdb, stream, group)
	if age < 0 {
		return
	}
	force := age > float64(lagSec)
	val := "0"
	ttl := time.Duration(lagSec) * time.Second
	if force {
		val = "1"
		ttl = 2 * time.Duration(lagSec) * time.Second
	}
	if err := rdb.Set(ctx, fraudAggForceKey, val, ttl).Err(); err != nil {
		slog.Debug("fraud agg force publish failed", "error", err)
	}
}

func oldestPELIdleSeconds(ctx context.Context, rdb redis.UniversalClient, stream, group string) float64 {
	pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  1,
	}).Result()
	if err != nil || len(pending) == 0 {
		return -1
	}
	return pending[0].Idle.Seconds()
}

func publishStreamPELAges(ctx context.Context, cfg FraudBackpressureConfig) {
	streams := []string{}
	if cfg.EventStream != "" {
		streams = append(streams, cfg.EventStream)
	}
	if cfg.Stream != "" {
		streams = append(streams, cfg.Stream)
	}
	group := cfg.Group
	if group == "" {
		group = "espx"
	}
	for i, rdb := range cfg.Rdbs {
		if rdb == nil {
			continue
		}
		shard := strconv.Itoa(i)
		for _, stream := range streams {
			age := oldestPELIdleSeconds(ctx, rdb, stream, group)
			if age < 0 {
				age = 0
			}
			metrics.FraudStreamPELAgeSeconds.WithLabelValues(stream, shard).Set(age)
		}
	}
}

// StartFraudLagPublisher runs on processor: samples fraud stream PEL and signals trackers.
func StartFraudLagPublisher(ctx context.Context, rdbs []redis.UniversalClient, stream, group string, lagSec int, interval time.Duration) {
	if len(rdbs) == 0 || stream == "" {
		return
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if lagSec <= 0 {
		lagSec = 30
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, rdb := range rdbs {
					if rdb == nil {
						continue
					}
					PublishFraudConsumerLag(ctx, rdb, stream, group, lagSec)
					break // one publish to shared key is enough
				}
			}
		}
	}()
}

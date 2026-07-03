package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"espx/internal/config"

	redis "github.com/redis/go-redis/v9"
)

const redisConnectRetries = 30

// RedisShardOptions tunes per-shard pool sizing and tracker filter deadlines.
type RedisShardOptions struct {
	PoolSize        int
	FilterTimeoutMs int // tracker hot path: aligns Read/WriteTimeout with filter deadline; 0 = default
}

// ConnectRedisShards dials every Redis shard with optional Sentinel failover and per-shard circuit breakers.
func ConnectRedisShards(ctx context.Context, cfg *config.Config, opts RedisShardOptions) ([]redis.UniversalClient, error) {
	names := cfg.ResolveRedisMasterNames()
	if cfg.RedisSentinelEnabled() && len(names) != len(cfg.RedisAddrs) {
		return nil, fmt.Errorf("sentinel master name count (%d) must match REDIS_ADDRS (%d)", len(names), len(cfg.RedisAddrs))
	}

	clients := make([]redis.UniversalClient, 0, len(cfg.RedisAddrs))
	for i := range cfg.RedisAddrs {
		rdb, err := connectRedisShard(ctx, cfg, i, names, opts)
		if err != nil {
			for _, c := range clients {
				_ = c.Close()
			}
			return nil, err
		}
		clients = append(clients, rdb)
	}
	return clients, nil
}

// ConnectRedisShard dials a single shard (auth and other single-shard services).
func ConnectRedisShard(ctx context.Context, cfg *config.Config, shardIdx int, opts RedisShardOptions) (redis.UniversalClient, error) {
	if shardIdx < 0 || shardIdx >= len(cfg.RedisAddrs) {
		return nil, fmt.Errorf("redis shard index %d out of range [0,%d)", shardIdx, len(cfg.RedisAddrs))
	}
	return connectRedisShard(ctx, cfg, shardIdx, cfg.ResolveRedisMasterNames(), opts)
}

func connectRedisShard(ctx context.Context, cfg *config.Config, shardIdx int, masterNames []string, opts RedisShardOptions) (redis.UniversalClient, error) {
	uopts := shardUniversalOptions(cfg, shardIdx, masterNames, opts)
	rdb := redis.NewUniversalClient(uopts)

	dialLabel := cfg.RedisAddrs[shardIdx]
	if cfg.RedisSentinelEnabled() {
		dialLabel = masterNames[shardIdx]
	}

	var pingErr error
	for attempt := 0; attempt < redisConnectRetries; attempt++ {
		if pingErr = rdb.Ping(ctx).Err(); pingErr == nil {
			break
		}
		slog.Warn("waiting for redis...", "shard", shardIdx, "target", dialLabel, "error", pingErr)
		time.Sleep(time.Second)
	}
	if pingErr != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis shard %d (%s): %w", shardIdx, dialLabel, pingErr)
	}

	breaker := NewRedisBreaker(
		int64(cfg.RedisBreakerFailThreshold),
		int64(cfg.RedisBreakerHalfOpen),
		time.Duration(cfg.RedisBreakerOpenTimeoutMs)*time.Millisecond,
	)
	rdb.AddHook(NewRedisCircuitBreakerHook(breaker))
	return rdb, nil
}

func shardUniversalOptions(cfg *config.Config, shardIdx int, masterNames []string, opts RedisShardOptions) *redis.UniversalOptions {
	uopts := &redis.UniversalOptions{
		Password: string(cfg.RedisPassword),
		PoolSize: opts.PoolSize,
	}
	if opts.FilterTimeoutMs > 0 {
		d := time.Duration(opts.FilterTimeoutMs) * time.Millisecond
		uopts.ReadTimeout = d
		uopts.WriteTimeout = d
	}
	if cfg.RedisSentinelEnabled() {
		uopts.MasterName = masterNames[shardIdx]
		uopts.Addrs = cfg.RedisSentinelAddrs
		return uopts
	}
	uopts.Addrs = []string{cfg.RedisAddrs[shardIdx]}
	return uopts
}

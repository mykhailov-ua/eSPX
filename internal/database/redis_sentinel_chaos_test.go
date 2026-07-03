package database

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"espx/internal/config"
)

func sentinelChaosConfig(t *testing.T) *config.Config {
	t.Helper()
	if os.Getenv("SENTINEL_CHAOS") == "" {
		t.Skip("set SENTINEL_CHAOS=1 (run scripts/test-sentinel-failover.sh or CI sentinel job)")
	}
	if testing.Short() {
		t.Skip("sentinel chaos skipped in -short")
	}

	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		t.Fatal("REDIS_PASSWORD required for sentinel chaos test")
	}

	addrs := splitEnvCSV(os.Getenv("REDIS_ADDRS"))
	sentinelAddrs := splitEnvCSV(os.Getenv("REDIS_SENTINEL_ADDRS"))
	masterNames := splitEnvCSV(os.Getenv("REDIS_MASTER_NAMES"))
	if len(addrs) == 0 || len(sentinelAddrs) == 0 || len(masterNames) != len(addrs) {
		t.Fatalf("need REDIS_ADDRS, REDIS_SENTINEL_ADDRS, REDIS_MASTER_NAMES (got %d/%d/%d addrs)",
			len(addrs), len(sentinelAddrs), len(masterNames))
	}

	return &config.Config{
		RedisAddrs:         addrs,
		RedisSentinelAddrs: sentinelAddrs,
		RedisMasterNames:   masterNames,
		RedisPassword:      config.Secret(password),
	}
}

func splitEnvCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// TestSentinelConnectAllShards verifies go-redis can reach every shard via Sentinel (pre-failover).
func TestSentinelConnectAllShards(t *testing.T) {
	cfg := sentinelChaosConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clients, err := ConnectRedisShards(ctx, cfg, RedisShardOptions{PoolSize: 4})
	if err != nil {
		t.Fatalf("ConnectRedisShards: %v", err)
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	for i, c := range clients {
		if err := c.Ping(ctx).Err(); err != nil {
			t.Fatalf("shard %d ping: %v", i, err)
		}
	}
}

// TestSentinelShard0SurvivesFailover expects redis-0 stopped and replica promoted (SENTINEL_FAILOVER_DONE=1).
func TestSentinelShard0SurvivesFailover(t *testing.T) {
	if os.Getenv("SENTINEL_FAILOVER_DONE") != "1" {
		t.Skip("orchestrator must stop redis-0 and set SENTINEL_FAILOVER_DONE=1")
	}
	cfg := sentinelChaosConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rdb, err := ConnectRedisShard(ctx, cfg, 0, RedisShardOptions{PoolSize: 4})
	if err != nil {
		t.Fatalf("ConnectRedisShard: %v", err)
	}
	defer func() { _ = rdb.Close() }()

	const key = "sentinel:chaos:marker"
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("GET %s after failover: %v", key, err)
	}
	if val != "ok" {
		t.Fatalf("GET %s = %q, want ok", key, val)
	}
}

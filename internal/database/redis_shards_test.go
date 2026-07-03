package database

import (
	"testing"

	"espx/internal/config"
)

func TestShardUniversalOptions_direct(t *testing.T) {
	cfg := &config.Config{
		RedisAddrs:    []string{"127.0.0.1:6479", "127.0.0.1:6480"},
		RedisPassword: "secret",
	}
	opts := shardUniversalOptions(cfg, 1, cfg.ResolveRedisMasterNames(), RedisShardOptions{PoolSize: 8, FilterTimeoutMs: 12})
	if opts.MasterName != "" {
		t.Fatalf("direct mode must not set MasterName, got %q", opts.MasterName)
	}
	if len(opts.Addrs) != 1 || opts.Addrs[0] != "127.0.0.1:6480" {
		t.Fatalf("addrs=%v", opts.Addrs)
	}
	if opts.PoolSize != 8 {
		t.Fatalf("pool=%d", opts.PoolSize)
	}
	if opts.ReadTimeout.Milliseconds() != 12 || opts.WriteTimeout.Milliseconds() != 12 {
		t.Fatalf("timeouts read=%s write=%s", opts.ReadTimeout, opts.WriteTimeout)
	}
}

func TestShardUniversalOptions_sentinel(t *testing.T) {
	cfg := &config.Config{
		RedisAddrs:         []string{"127.0.0.1:6479", "127.0.0.1:6480"},
		RedisSentinelAddrs: []string{"127.0.0.1:26379", "127.0.0.1:26380"},
		RedisMasterNames:   []string{"espx-shard-0", "espx-shard-1"},
		RedisPassword:      "secret",
	}
	opts := shardUniversalOptions(cfg, 0, cfg.RedisMasterNames, RedisShardOptions{})
	if opts.MasterName != "espx-shard-0" {
		t.Fatalf("master=%q", opts.MasterName)
	}
	if len(opts.Addrs) != 2 {
		t.Fatalf("sentinel addrs=%v", opts.Addrs)
	}
}

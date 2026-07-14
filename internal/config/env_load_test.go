package config

import (
	"strings"
	"testing"
)

func TestTrimCommaList_dropsEmpty(t *testing.T) {
	got := trimCommaList(" a , ,b, ")
	want := []string{"a", "b"}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%q want %q", i, got[i], want[i])
		}
	}
}

func TestResolveRedisMasterNames_defaults(t *testing.T) {
	cfg := &Config{RedisAddrs: []string{"h0:6379", "h1:6379", "h2:6379"}}
	names := cfg.ResolveRedisMasterNames()
	want := []string{"espx-shard-0", "espx-shard-1", "espx-shard-2"}
	if len(names) != len(want) {
		t.Fatalf("len=%d want %d", len(names), len(want))
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("names[%d]=%q want %q", i, names[i], want[i])
		}
	}
}

func TestResolveRedisMasterNames_explicit(t *testing.T) {
	cfg := &Config{
		RedisAddrs:       []string{"h0:6379", "h1:6379"},
		RedisMasterNames: []string{"custom-a", "custom-b"},
	}
	names := cfg.ResolveRedisMasterNames()
	if names[0] != "custom-a" || names[1] != "custom-b" {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestRedisSentinelEnabled(t *testing.T) {
	if (&Config{}).RedisSentinelEnabled() {
		t.Fatal("expected disabled with empty sentinel addrs")
	}
	if !(&Config{RedisSentinelAddrs: []string{"127.0.0.1:26379"}}).RedisSentinelEnabled() {
		t.Fatal("expected enabled when sentinel addrs set")
	}
}

func TestLoad_productionRequiresExpectedShardCount(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("SERVER_PORT", "8181")
	t.Setenv("DB_DSN", "postgres://u:p@localhost/db?sslmode=disable")
	t.Setenv("REDIS_ADDRS", "127.0.0.1:6379")
	t.Setenv("TOKEN_SYMMETRIC_KEY", "01234567890123456789012345678901")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for single shard in production")
	}

	t.Setenv("REDIS_ADDRS", strings.Join([]string{
		"127.0.0.1:6479", "127.0.0.1:6480", "127.0.0.1:6481", "127.0.0.1:6482",
	}, ","))
	t.Setenv("FILTER_TIMEOUT_MS", "100")
	_, err = Load()
	if err != nil {
		t.Fatalf("expected load ok with %d shards: %v", ExpectedRedisShardCount, err)
	}
}

func TestLoad_productionFilterTimeoutCeiling(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("SERVER_PORT", "8181")
	t.Setenv("DB_DSN", "postgres://u:p@localhost/db?sslmode=disable")
	t.Setenv("REDIS_ADDRS", "127.0.0.1:6479,127.0.0.1:6480,127.0.0.1:6481,127.0.0.1:6482")
	t.Setenv("TOKEN_SYMMETRIC_KEY", "01234567890123456789012345678901")
	t.Setenv("FILTER_TIMEOUT_MS", "101")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "FILTER_TIMEOUT_MS") {
		t.Fatalf("expected FILTER_TIMEOUT_MS error, got %v", err)
	}

	t.Setenv("FILTER_TIMEOUT_MS", "100")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FilterTimeoutMs != 100 {
		t.Fatalf("FilterTimeoutMs=%d want 100", cfg.FilterTimeoutMs)
	}
}

func TestMLAnalyticsEnabled_defaultFalse(t *testing.T) {
	cfg := &Config{}
	if cfg.MLAnalyticsEnabled() {
		t.Fatal("ML_ANALYTICS_ENABLED must default to false")
	}

	t.Setenv("ML_ANALYTICS_ENABLED", "true")
	cfg2 := &Config{}
	cfg2.ML.Enabled = getEnvBool("ML_ANALYTICS_ENABLED", false)
	if !cfg2.MLAnalyticsEnabled() {
		t.Fatal("ML_ANALYTICS_ENABLED=true must enable analytics")
	}
}

package catalog

import (
	"context"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards runtime config reload from Redis updates hot-path amounts without restart.
func TestSettingsWatcher(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.Config{
		RateLimitPerMin:  100,
		ClickAmount:      100_000,
		ImpressionAmount: 10_000,
	}

	sw := NewSettingsWatcher([]redis.UniversalClient{rdb}, cfg)

	assert.Equal(t, 100, sw.Get().RateLimitPerMin)
	assert.Equal(t, int64(100_000), sw.Get().ClickAmount)

	go sw.Start(ctx, 100*time.Millisecond)

	err := rdb.HSet(ctx, "config:values", map[string]interface{}{
		"rate_limit_per_min": "200",
		"click_amount":       "0.25",
	}).Err()
	require.NoError(t, err)

	err = rdb.Incr(ctx, "config:version").Err()
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return sw.Get().RateLimitPerMin == 200 && sw.Get().ClickAmount == 250_000
	}, 2*time.Second, 200*time.Millisecond)

	assert.Equal(t, int64(1), sw.Get().Version)
}

// Guards SettingsWatcher reads config from a secondary shard when the first is unavailable.
func TestSettingsWatcher_shardFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	var endpoint string
	switch client := rdb.(type) {
	case *redis.Client:
		endpoint = client.Options().Addr
	default:
		t.Fatalf("unexpected redis client type")
	}

	rdbDead := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"127.0.0.1:1"}})
	defer rdbDead.Close()

	rdbLive := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}, DB: 3})
	defer rdbLive.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.Config{
		RateLimitPerMin:  100,
		ClickAmount:      100_000,
		ImpressionAmount: 10_000,
	}

	sw := NewSettingsWatcher([]redis.UniversalClient{rdbDead, rdbLive}, cfg)
	go sw.Start(ctx, 50*time.Millisecond)

	require.NoError(t, rdbLive.HSet(ctx, "config:values", map[string]interface{}{
		"rate_limit_per_min": "300",
	}).Err())
	require.NoError(t, rdbLive.Set(ctx, "config:version", 5, 0).Err())

	assert.Eventually(t, func() bool {
		return sw.Get().RateLimitPerMin == 300 && sw.Get().Version == 5
	}, 2*time.Second, 50*time.Millisecond)
}

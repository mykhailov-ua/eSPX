package management

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncGlobalConfigToAllShards(t *testing.T) {
	ctx := context.Background()
	rdb1 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 14})
	rdb2 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 15})
	if err := rdb1.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available:", err)
	}
	t.Cleanup(func() {
		_ = rdb1.FlushDB(ctx).Err()
		_ = rdb2.FlushDB(ctx).Err()
		_ = rdb1.Close()
		_ = rdb2.Close()
	})

	settings := map[string]string{
		"emergency_breaker":  "true",
		"rate_limit_per_min": "42",
	}
	require.NoError(t, syncGlobalConfigToAllShards(ctx, []redis.UniversalClient{rdb1, rdb2}, settings, 99))

	for i, rdb := range []redis.UniversalClient{rdb1, rdb2} {
		val, err := rdb.HGet(ctx, redisConfigValuesKey, "emergency_breaker").Result()
		require.NoError(t, err, "shard %d", i)
		assert.Equal(t, "true", val)

		version, err := rdb.Get(ctx, redisConfigVersionKey).Int64()
		require.NoError(t, err, "shard %d", i)
		assert.Equal(t, int64(99), version)
	}
}

func TestReplicateConfigVersionFromPrimary(t *testing.T) {
	ctx := context.Background()
	rdb1 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 12})
	rdb2 := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 13})
	if err := rdb1.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available:", err)
	}
	t.Cleanup(func() {
		_ = rdb1.FlushDB(ctx).Err()
		_ = rdb2.FlushDB(ctx).Err()
		_ = rdb1.Close()
		_ = rdb2.Close()
	})

	require.NoError(t, rdb1.Set(ctx, redisConfigVersionKey, 7, 0).Err())
	require.NoError(t, replicateConfigVersionFromPrimary(ctx, []redis.UniversalClient{rdb1, rdb2}))

	version, err := rdb2.Get(ctx, redisConfigVersionKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(7), version)
}

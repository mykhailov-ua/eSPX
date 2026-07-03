package testutil

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// SetupRedis boots an isolated Redis testcontainer.
func SetupRedis(t testing.TB) (redis.UniversalClient, func()) {
	t.Helper()
	_, client, cleanup := SetupRedisContainer(t)
	return client, cleanup
}

// SetupRedisContainer boots Redis and returns the container for fault injection.
func SetupRedisContainer(t testing.TB) (testcontainers.Container, redis.UniversalClient, func()) {
	t.Helper()
	ctx := context.Background()

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %s", err)
	}

	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %s", err)
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{endpoint},
	})

	cleanup := func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
	return redisContainer, rdb, cleanup
}

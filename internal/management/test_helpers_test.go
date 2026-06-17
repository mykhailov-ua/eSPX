package management

import (
	"context"
	"strings"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// newBareService exists so chaos tests avoid background workers that contend on the database pool.
func newBareService(t *testing.T, pool *pgxpool.Pool, rdbs []redis.UniversalClient, cfg *config.Config) *Service {
	t.Helper()
	if cfg == nil {
		cfg = &config.Config{}
	}
	shardCount := len(rdbs)
	if shardCount == 0 {
		shardCount = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	svc := &Service{
		pool:    pool,
		rdbs:    rdbs,
		sharder: ads.NewJumpHashSharder(shardCount),
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
	t.Cleanup(func() {
		cancel()
		svc.Close()
	})
	return svc
}

// isDeadlock exists so chaos tests can retry transient Postgres deadlock conflicts instead of failing.
func isDeadlock(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "deadlock detected") || strings.Contains(msg, "40P01")
}

// slowRedisClient injects latency into Redis calls to exercise outbox timeout and retry behavior.
type slowRedisClient struct {
	redis.UniversalClient
	delay time.Duration
}

func (c *slowRedisClient) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.Pipelined(ctx, fn)
}

func (c *slowRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.Set(ctx, key, value, expiration)
}

func (c *slowRedisClient) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.SAdd(ctx, key, members...)
}

func (c *slowRedisClient) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.SRem(ctx, key, members...)
}

func (c *slowRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.Del(ctx, keys...)
}

func (c *slowRedisClient) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.Publish(ctx, channel, message)
}

func (c *slowRedisClient) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.HSet(ctx, key, values...)
}

func (c *slowRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.Get(ctx, key)
}

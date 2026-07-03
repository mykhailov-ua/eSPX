package tests

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newTestRegistry isolates registry sync from production replica paths so tests
// never overwrite or read live campaign snapshots on disk.
func newTestRegistry(t *testing.T, repo db.Querier) *ads.CampaignRegistry {
	t.Helper()
	r := ads.NewRegistry(repo)
	r.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	return r
}

// setupTestDB boots a real Postgres with the full migration chain because sqlc
// queries and constraints are only trustworthy against the actual schema.
func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %s", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to db: %s", err)
	}

	migrationsDir := filepath.Join("..", "internal/ads", "migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("failed to read migrations dir: %s", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir, entry.Name()))
		if err != nil {
			t.Fatalf("failed to read migration %s: %s", entry.Name(), err)
		}

		sql := string(sqlBytes)
		parts := strings.Split(sql, "-- +goose Down")
		upPart := parts[0]
		upPart = strings.ReplaceAll(upPart, "-- +goose Up", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementBegin", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementEnd", "")

		if _, err := pool.Exec(ctx, upPart); err != nil {
			t.Fatalf("failed to apply migration %s: %s", entry.Name(), err)
		}
	}

	return pool, func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
}

// setupTestRedis provides a live Redis instance so Lua scripts, streams, and
// atomic budget keys behave like production instead of an in-process stub.
func setupTestRedis(t *testing.T) (redis.UniversalClient, func()) {
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

	return rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

// setupTestRedisClientContainer is like setupTestRedis but returns *redis.Client for breaker hooks.
func setupTestRedisClientContainer(t *testing.T) (testcontainers.Container, *redis.Client, func()) {
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

	rdb := redis.NewClient(&redis.Options{
		Addr:         endpoint,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})

	return redisContainer, rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

// setupTestRedisShards starts one Redis testcontainer per logical shard (production topology).
func setupTestRedisShards(t *testing.T, n int) []redis.UniversalClient {
	t.Helper()
	shards := make([]redis.UniversalClient, n)
	for i := range shards {
		rdb, cleanup := setupTestRedis(t)
		t.Cleanup(cleanup)
		shards[i] = rdb
	}
	return shards
}

// campaignIDForShard returns a UUID that sharder routes to wantShard.
func campaignIDForShard(t *testing.T, sharder ads.Sharder, wantShard int) uuid.UUID {
	t.Helper()
	for range 20_000 {
		id := uuid.New()
		if sharder.GetShard(id) == wantShard {
			return id
		}
	}
	t.Fatalf("could not find campaign id for shard %d", wantShard)
	return uuid.Nil
}

const redisShardStopTimeout = 10 * time.Second

// redisShardChaosInfra holds per-shard Redis clients, containers, and circuit breakers for outage tests.
type redisShardChaosInfra struct {
	Clients    []*redis.Client
	Containers []testcontainers.Container
	Breakers   []*database.RedisBreaker
	cleanups   []func()
}

// setupTestRedisShardsChaos starts isolated Redis containers with fast-fail clients and per-shard breakers.
func setupTestRedisShardsChaos(t *testing.T, n int) *redisShardChaosInfra {
	t.Helper()
	ctx := context.Background()
	infra := &redisShardChaosInfra{
		Clients:    make([]*redis.Client, n),
		Containers: make([]testcontainers.Container, n),
		Breakers:   make([]*database.RedisBreaker, n),
	}
	for i := range infra.Clients {
		c, rdb, cleanup := setupTestRedisClientContainer(t)
		infra.Containers[i] = c
		infra.Clients[i] = rdb
		infra.cleanups = append(infra.cleanups, cleanup)

		breaker := database.NewRedisBreaker(3, 2, 300*time.Millisecond)
		infra.Breakers[i] = breaker
		infra.Clients[i].AddHook(database.NewRedisCircuitBreakerHook(breaker, strconv.Itoa(i)))

		require.NoError(t, infra.Clients[i].Ping(ctx).Err())
	}
	t.Cleanup(infra.cleanup)
	return infra
}

func (infra *redisShardChaosInfra) UniversalClients() []redis.UniversalClient {
	out := make([]redis.UniversalClient, len(infra.Clients))
	for i, c := range infra.Clients {
		out[i] = c
	}
	return out
}

func (infra *redisShardChaosInfra) cleanup() {
	for _, fn := range infra.cleanups {
		fn()
	}
}

func (infra *redisShardChaosInfra) replaceShardClient(t *testing.T, idx int, rdbs []redis.UniversalClient) {
	t.Helper()
	ctx := context.Background()
	endpoint, err := infra.Containers[idx].Endpoint(ctx, "")
	require.NoError(t, err)
	_ = infra.Clients[idx].Close()

	client := redis.NewClient(&redis.Options{
		Addr:         endpoint,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})
	breaker := database.NewRedisBreaker(3, 2, 300*time.Millisecond)
	client.AddHook(database.NewRedisCircuitBreakerHook(breaker, strconv.Itoa(idx)))

	infra.Clients[idx] = client
	infra.Breakers[idx] = breaker
	if rdbs != nil && idx < len(rdbs) {
		rdbs[idx] = client
	}
	require.NoError(t, client.Ping(ctx).Err())
}

func waitRedisContainerReady(t *testing.T, c testcontainers.Container) {
	t.Helper()
	require.Eventually(t, func() bool {
		ctx := context.Background()
		endpoint, err := c.Endpoint(ctx, "")
		if err != nil {
			return false
		}
		probe := redis.NewClient(&redis.Options{Addr: endpoint, ReadTimeout: time.Second})
		defer probe.Close()
		return probe.Ping(ctx).Err() == nil
	}, 30*time.Second, 200*time.Millisecond)
}

func stopRedisShardContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	timeout := redisShardStopTimeout
	require.NoError(t, c.Stop(context.Background(), &timeout))
}

func startRedisShardContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	require.NoError(t, c.Start(context.Background()))
}

func waitRedisClientReady(t *testing.T, rdb redis.UniversalClient) {
	t.Helper()
	require.Eventually(t, func() bool {
		return rdb.Ping(context.Background()).Err() == nil
	}, 30*time.Second, 100*time.Millisecond)
}

func tripRedisBreaker(t *testing.T, rdb redis.UniversalClient, breaker *database.RedisBreaker) {
	t.Helper()
	ctx := context.Background()
	require.Eventually(t, func() bool {
		for range 3 {
			_ = rdb.Ping(ctx).Err()
		}
		return breaker.State() == database.CircuitOpen
	}, 10*time.Second, 50*time.Millisecond)
}

func waitRedisBreakerClosed(t *testing.T, rdb redis.UniversalClient, breaker *database.RedisBreaker) {
	t.Helper()
	ctx := context.Background()
	require.Eventually(t, func() bool {
		_ = rdb.Ping(ctx).Err()
		return breaker.State() == database.CircuitClosed
	}, 15*time.Second, 100*time.Millisecond)
}

func latencyBudget(baseline time.Duration) time.Duration {
	if baseline < time.Millisecond {
		baseline = time.Millisecond
	}
	return baseline * 2
}

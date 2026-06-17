package management

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const mgmtContainerStopTimeout = 10 * time.Second

// mgmtChaosInfra holds live Postgres and Redis with container handles for fault injection.
type mgmtChaosInfra struct {
	Pool           *pgxpool.Pool
	Redis          redis.UniversalClient
	PGContainer    *postgres.PostgresContainer
	RedisContainer testcontainers.Container
}

// setupMgmtChaosInfra boots Postgres and Redis with ads migrations applied.
func setupMgmtChaosInfra(t *testing.T) (*mgmtChaosInfra, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("mgmt_chaos_db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(20*time.Second)),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	applyMgmtChaosMigrations(t, pool)

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)

	endpoint, err := redisContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}})
	require.NoError(t, rdb.Ping(ctx).Err())

	infra := &mgmtChaosInfra{
		Pool:           pool,
		Redis:          rdb,
		PGContainer:    pgContainer,
		RedisContainer: redisContainer,
	}

	cleanup := func() {
		_ = rdb.Close()
		pool.Close()
		_ = redisContainer.Terminate(ctx)
		_ = pgContainer.Terminate(ctx)
	}
	return infra, cleanup
}

func applyMgmtChaosMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	migrationsDir := filepath.Join(filepath.Dir(filename), "..", "ads", "migrations")
	entries, err := os.ReadDir(migrationsDir)
	require.NoError(t, err)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir, entry.Name()))
		require.NoError(t, err)

		sql := string(sqlBytes)
		parts := strings.Split(sql, "-- +goose Down")
		upPart := parts[0]
		upPart = strings.ReplaceAll(upPart, "-- +goose Up", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementBegin", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementEnd", "")

		_, err = pool.Exec(ctx, upPart)
		require.NoError(t, err, "migration %s", entry.Name())
	}
}

func stopMgmtContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	timeout := mgmtContainerStopTimeout
	require.NoError(t, c.Stop(context.Background(), &timeout))
}

func startMgmtContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	require.NoError(t, c.Start(context.Background()))
}

func waitMgmtPGReady(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.Eventually(t, func() bool {
		return pool.Ping(context.Background()) == nil
	}, 30*time.Second, 200*time.Millisecond)
}

func waitMgmtRedisReady(t *testing.T, rdb redis.UniversalClient) {
	t.Helper()
	require.Eventually(t, func() bool {
		return rdb.Ping(context.Background()).Err() == nil
	}, 30*time.Second, 200*time.Millisecond)
}

func (infra *mgmtChaosInfra) refreshRedisClient(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_ = infra.Redis.Close()
	endpoint, err := infra.RedisContainer.Endpoint(ctx, "")
	require.NoError(t, err)
	infra.Redis = redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}})
	waitMgmtRedisReady(t, infra.Redis)
}

func (infra *mgmtChaosInfra) refreshPGPool(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	infra.Pool.Close()
	connStr, err := infra.PGContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	infra.Pool = pool
	waitMgmtPGReady(t, infra.Pool)
}

func requireMgmtFaultActive(t *testing.T, faultActive func() bool, msg string) {
	t.Helper()
	require.Eventually(t, faultActive, 10*time.Second, 100*time.Millisecond, msg)
}

func rebindBareService(svc *Service, infra *mgmtChaosInfra) {
	svc.pool = infra.Pool
	svc.rdbs = []redis.UniversalClient{infra.Redis}
}

func outboxStatus(t *testing.T, pool *pgxpool.Pool, eventID int64) string {
	t.Helper()
	var status string
	err := pool.QueryRow(context.Background(),
		`SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&status)
	require.NoError(t, err)
	return status
}

func latestOutboxEventID(t *testing.T, pool *pgxpool.Pool, eventType string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), `
		SELECT id FROM outbox_events WHERE event_type = $1 ORDER BY id DESC LIMIT 1`, eventType).Scan(&id)
	require.NoError(t, err)
	return id
}

func itoaMgmtChaos(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

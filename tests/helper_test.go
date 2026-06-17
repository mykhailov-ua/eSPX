package tests

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
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

// setupTestDBContainer is like setupTestDB but returns the container for chaos fault injection.
func setupTestDBContainer(t *testing.T) (testcontainers.Container, *pgxpool.Pool, func()) {
	t.Helper()
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

	return pgContainer, pool, func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
}

// setupTestRedisContainer is like setupTestRedis but returns the container for chaos fault injection.
func setupTestRedisContainer(t *testing.T) (testcontainers.Container, redis.UniversalClient, func()) {
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

	return redisContainer, rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

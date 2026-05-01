package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

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

	path := filepath.Join("..", "..", "internal/ads/repository", "migrations", "00001_init_schema.sql")
	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read migration from %s: %s", path, err)
	}

	sql := string(sqlBytes)
	parts := strings.Split(sql, "-- +goose Down")
	upPart := parts[0]
	upPart = strings.ReplaceAll(upPart, "-- +goose Up", "")
	upPart = strings.ReplaceAll(upPart, "-- +goose StatementBegin", "")
	upPart = strings.ReplaceAll(upPart, "-- +goose StatementEnd", "")

	if _, err := pool.Exec(ctx, upPart); err != nil {
		t.Fatalf("failed to apply migration: %s", err)
	}

	return pool, func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
}

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

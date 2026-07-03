package testutil

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PostgresConfig tunes the ephemeral Postgres testcontainer.
type PostgresConfig struct {
	DatabaseName  string
	Username      string
	Password      string
	MigrationDirs []string
}

func DefaultPostgresConfig() PostgresConfig {
	return PostgresConfig{
		DatabaseName:  "testdb",
		Username:      "user",
		Password:      "pass",
		MigrationDirs: []string{AdsMigrationsDir()},
	}
}

func SetupAdsPostgres(t testing.TB) (*pgxpool.Pool, func()) {
	t.Helper()
	pool, cleanup := SetupPostgres(t, DefaultPostgresConfig())
	return pool, cleanup
}

func SetupAdsPaymentPostgres(t testing.TB) (*pgxpool.Pool, func()) {
	t.Helper()
	cfg := DefaultPostgresConfig()
	cfg.DatabaseName = "payment_test_db"
	cfg.Username = "postgres"
	cfg.Password = "secure_password"
	cfg.MigrationDirs = []string{AdsMigrationsDir(), PaymentMigrationsDir()}
	pool, cleanup := SetupPostgres(t, cfg)
	return pool, cleanup
}

func SetupPostgres(t testing.TB, cfg PostgresConfig) (*pgxpool.Pool, func()) {
	t.Helper()
	_, pool, cleanup := setupPostgresContainer(t, cfg)
	return pool, cleanup
}

func SetupAdsPostgresContainer(t testing.TB) (*postgres.PostgresContainer, *pgxpool.Pool, func()) {
	t.Helper()
	return setupPostgresContainer(t, DefaultPostgresConfig())
}

func SetupFaultPostgresContainer(t testing.TB, databaseName string) (*postgres.PostgresContainer, *pgxpool.Pool, func()) {
	t.Helper()
	cfg := DefaultPostgresConfig()
	if databaseName != "" {
		cfg.DatabaseName = databaseName
	}
	return setupPostgresContainer(t, cfg)
}

func setupPostgresContainer(t testing.TB, cfg PostgresConfig) (*postgres.PostgresContainer, *pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(cfg.DatabaseName),
		postgres.WithUsername(cfg.Username),
		postgres.WithPassword(cfg.Password),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(20*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %s", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get postgres connection string: %s", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %s", err)
	}

	for _, dir := range cfg.MigrationDirs {
		ApplyMigrations(t, pool, dir)
	}

	cleanup := func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
	return pgContainer, pool, cleanup
}

func ApplyMigrations(t testing.TB, pool *pgxpool.Pool, dir string) {
	t.Helper()
	ctx := context.Background()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read migrations dir %s: %s", dir, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := os.ReadFile(filepath.Join(dir, entry.Name()))
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
}

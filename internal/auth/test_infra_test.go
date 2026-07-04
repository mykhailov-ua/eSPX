package auth

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"espx/internal/auth/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const testPasetoKey = "yellow-submarine-yellow-submarin"

// authTestInfra holds live Postgres and Redis for integration and chaos tests.
type authTestInfra struct {
	Pool           *pgxpool.Pool
	Redis          redis.UniversalClient
	Store          db.Store
	PGContainer    *postgres.PostgresContainer
	RedisContainer testcontainers.Container
}

// setupAuthTestInfra boots Postgres and Redis with auth migrations applied.
func setupAuthTestInfra(t *testing.T) (*authTestInfra, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("auth_test_db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("secure_password"),
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
	applyAuthMigrations(t, pool)

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)

	endpoint, err := redisContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}})
	require.NoError(t, rdb.Ping(ctx).Err())

	infra := &authTestInfra{
		Pool:           pool,
		Redis:          rdb,
		Store:          db.NewStore(pool),
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

// newService wires a production-like auth service against live test infra.
func (infra *authTestInfra) newService(t *testing.T) *Service {
	t.Helper()
	tokenMaker, err := NewPasetoMaker(testPasetoKey)
	require.NoError(t, err)
	hasher, err := NewPasswordHasher(4096, 1, 1)
	require.NoError(t, err)
	lockout := NewLockoutLimiter(infra.Redis)
	return NewService(infra.Store, tokenMaker, hasher, lockout, infra.Redis)
}

// registerAndLogin provisions a verified user and returns tokens from a successful login.
func (infra *authTestInfra) registerAndLogin(t *testing.T, svc *Service, email, password string) (uuid.UUID, string, string) {
	t.Helper()
	ctx := context.Background()
	userID, err := svc.Register(ctx, RegisterDTO{Email: email, Password: password, Role: RoleUser})
	require.NoError(t, err)

	resp, err := svc.Login(ctx, email, password, "test-agent", "10.0.0.1", time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, resp.AccessToken)
	require.NotEmpty(t, resp.RefreshToken)
	return userID, resp.AccessToken, resp.RefreshToken
}

const authContainerStopTimeout = 10 * time.Second

// stopAuthContainer pauses a dependency without destroying its data (recovery tests).
func stopAuthContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	timeout := authContainerStopTimeout
	require.NoError(t, c.Stop(context.Background(), &timeout))
}

// startAuthContainer brings a stopped dependency back online.
func startAuthContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	require.NoError(t, c.Start(context.Background()))
}

// waitAuthPGReady blocks until Postgres accepts connections again after stop/start.
func waitAuthPGReady(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.Eventually(t, func() bool {
		return pool.Ping(context.Background()) == nil
	}, 30*time.Second, 200*time.Millisecond)
}

// waitAuthRedisReady blocks until Redis accepts connections again after stop/start.
func waitAuthRedisReady(t *testing.T, rdb redis.UniversalClient) {
	t.Helper()
	require.Eventually(t, func() bool {
		return rdb.Ping(context.Background()).Err() == nil
	}, 30*time.Second, 200*time.Millisecond)
}

// refreshRedisClient re-resolves the container endpoint and replaces the pooled client.
func (infra *authTestInfra) refreshRedisClient(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_ = infra.Redis.Close()
	endpoint, err := infra.RedisContainer.Endpoint(ctx, "")
	require.NoError(t, err)
	infra.Redis = redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}})
	waitAuthRedisReady(t, infra.Redis)
}

// refreshPGPool re-resolves the container DSN and replaces the pgx pool.
func (infra *authTestInfra) refreshPGPool(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	infra.Pool.Close()
	connStr, err := infra.PGContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	infra.Pool = pool
	infra.Store = db.NewStore(pool)
	waitAuthPGReady(t, infra.Pool)
}

// countActiveSessions returns live refresh rows for a user after rotation.
func countActiveSessions(t *testing.T, pool *pgxpool.Pool, userID uuid.UUID) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM sessions WHERE user_id = $1 AND is_blocked = FALSE AND expires_at > NOW()`,
		userID,
	).Scan(&n)
	require.NoError(t, err)
	return n
}

// applyAuthMigrations runs goose up SQL from internal/auth/migrations against pool.
func applyAuthMigrations(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(filename), "migrations")

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("failed to read migrations dir %s: %s", migrationsDir, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

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
}

package auth

import (
	"context"
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

const chaosPasetoKey = "yellow-submarine-yellow-submarin"

// authChaosInfra holds live dependencies for container-level fault injection.
type authChaosInfra struct {
	Pool           *pgxpool.Pool
	Redis          redis.UniversalClient
	Store          db.Store
	PGContainer    testcontainers.Container
	RedisContainer testcontainers.Container
}

// setupAuthChaosInfra boots Postgres and Redis with auth migrations applied.
func setupAuthChaosInfra(t *testing.T) (*authChaosInfra, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("auth_chaos_db"),
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

	infra := &authChaosInfra{
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

// newChaosService wires a production-like auth service against chaos infra.
func (infra *authChaosInfra) newChaosService(t *testing.T) *Service {
	t.Helper()
	tokenMaker, err := NewPasetoMaker(chaosPasetoKey)
	require.NoError(t, err)
	hasher, err := NewPasswordHasher(4096, 1, 1)
	require.NoError(t, err)
	lockout := NewLockoutLimiter(infra.Redis)
	return NewService(infra.Store, tokenMaker, hasher, lockout, infra.Redis)
}

// registerAndLogin provisions a verified user and returns tokens from a successful login.
func (infra *authChaosInfra) registerAndLogin(t *testing.T, svc *Service, email, password string) (uuid.UUID, string, string) {
	t.Helper()
	ctx := context.Background()
	userID, err := svc.Register(ctx, RegisterDTO{Email: email, Password: password, Role: RoleUser})
	require.NoError(t, err)

	resp, err := svc.Login(ctx, email, password, "chaos-agent", "10.0.0.1", time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, resp.AccessToken)
	require.NotEmpty(t, resp.RefreshToken)
	return userID, resp.AccessToken, resp.RefreshToken
}

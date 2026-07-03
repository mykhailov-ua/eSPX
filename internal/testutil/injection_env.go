package testutil

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// InjectionEnv holds live Postgres and Redis containers for fault-injection tests.
type InjectionEnv struct {
	Pool           *pgxpool.Pool
	Redis          redis.UniversalClient
	PGContainer    *postgres.PostgresContainer
	RedisContainer testcontainers.Container
}

// NewInjectionEnv boots Postgres with ads migrations and Redis for fault-injection tests.
func NewInjectionEnv(t testing.TB) (*InjectionEnv, func()) {
	t.Helper()

	pgContainer, pool, cleanupPG := SetupAdsPostgresContainer(t)
	redisContainer, rdb, cleanupRedis := SetupRedisContainer(t)

	env := &InjectionEnv{
		Pool:           pool,
		Redis:          rdb,
		PGContainer:    pgContainer,
		RedisContainer: redisContainer,
	}
	cleanup := func() {
		cleanupRedis()
		cleanupPG()
	}
	return env, cleanup
}

// LogFaultProof emits a structured fault-injection record for CI grep (see Makefile test-fault).
func LogFaultProof(t testing.TB, scenario string, attrs map[string]string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("fault_proof fault=")
	b.WriteString(scenario)
	for k, v := range attrs {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
	}
	t.Log(b.String())
}

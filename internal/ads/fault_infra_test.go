package ads

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const adsContainerStopTimeout = 10 * time.Second

// adsChaosInfra holds live Postgres and Redis for ads chaos tests.
type adsChaosInfra struct {
	Pool           *pgxpool.Pool
	Redis          redis.UniversalClient
	Queries        db.Querier
	PGContainer    *postgres.PostgresContainer
	RedisContainer testcontainers.Container
}

// adsIngestStack wires tracker HTTP and a stream consumer against chaos infra.
type adsIngestStack struct {
	Srv        *httptest.Server
	Consumer   *StreamConsumer
	Registry   *CampaignRegistry
	CampaignID uuid.UUID
	Stream     string
	ctx        context.Context
	Cancel     context.CancelFunc
	cfg        *config.Config
}

// setupAdsChaosInfra boots Postgres and Redis with ads migrations applied.
func setupAdsChaosInfra(t *testing.T) (*adsChaosInfra, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("ads_chaos_db"),
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
	applyAdsMigrations(t, pool)

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)

	endpoint, err := redisContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}})
	require.NoError(t, rdb.Ping(ctx).Err())

	infra := &adsChaosInfra{
		Pool:           pool,
		Redis:          rdb,
		Queries:        db.New(pool),
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

func applyAdsMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	migrationsDir := filepath.Join(filepath.Dir(filename), "migrations")
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

func stopAdsContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	timeout := adsContainerStopTimeout
	require.NoError(t, c.Stop(context.Background(), &timeout))
}

func startAdsContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	require.NoError(t, c.Start(context.Background()))
}

func waitAdsPGReady(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	require.Eventually(t, func() bool {
		return pool.Ping(context.Background()) == nil
	}, 30*time.Second, 200*time.Millisecond)
}

func waitAdsRedisReady(t *testing.T, rdb redis.UniversalClient) {
	t.Helper()
	require.Eventually(t, func() bool {
		return rdb.Ping(context.Background()).Err() == nil
	}, 30*time.Second, 200*time.Millisecond)
}

func (infra *adsChaosInfra) refreshRedisClient(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_ = infra.Redis.Close()
	endpoint, err := infra.RedisContainer.Endpoint(ctx, "")
	require.NoError(t, err)
	infra.Redis = redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}})
	waitAdsRedisReady(t, infra.Redis)
}

func (infra *adsChaosInfra) refreshPGPool(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	infra.Pool.Close()
	connStr, err := infra.PGContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	infra.Pool = pool
	infra.Queries = db.New(pool)
	waitAdsPGReady(t, infra.Pool)
}

func requireAdsFaultActive(t *testing.T, faultActive func() bool, msg string) {
	t.Helper()
	require.Eventually(t, faultActive, 10*time.Second, 100*time.Millisecond, msg)
}

func newChaosRegistry(t *testing.T, queries db.Querier) *CampaignRegistry {
	t.Helper()
	r := NewRegistry(queries)
	r.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	return r
}

func seedChaosCampaign(t *testing.T, infra *adsChaosInfra, registry *CampaignRegistry) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	pm := database.NewPartitionManager(infra.Pool, 7, 1)
	require.NoError(t, pm.Run(ctx))

	customerID := uuid.New()
	_, err := infra.Pool.Exec(ctx,
		"INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)",
		customerID, "Chaos Customer", 1_000_000_000)
	require.NoError(t, err)

	campaignID := uuid.New()
	_, err = infra.Pool.Exec(ctx,
		"INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
		campaignID, "Chaos Campaign", "ACTIVE", customerID, 100_000_000)
	require.NoError(t, err)

	_, _ = registry.Sync(ctx)
	return campaignID
}

func startAdsIngestStack(t *testing.T, infra *adsChaosInfra, stream string) *adsIngestStack {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	cfg := &config.Config{
		EventBatchSize:     10,
		EventFlushMs:       100,
		StatsFlushMs:       100,
		MaxWorkers:         2,
		WriteTimeoutMs:     2000,
		FilterTimeoutMs:    2000,
		MaxRequestBodySize: 1024 * 1024,
		StreamMaxLen:       100000,
	}

	registry := newChaosRegistry(t, infra.Queries)
	campaignID := seedChaosCampaign(t, infra, registry)

	store := NewPostgresStore(infra.Queries, 1*time.Second)
	campaignRepo := NewCampaignRepo(infra.Queries)
	unifiedFilter := NewUnifiedFilter(
		[]redis.UniversalClient{infra.Redis},
		NewJumpHashSharder(1),
		registry,
		campaignRepo,
		1000,
		time.Minute,
		45*time.Second,
		24*time.Hour,
		100_000,
		10_000,
		stream,
		100000,
	)
	filterEngine := NewFilterEngine(time.Duration(cfg.FilterTimeoutMs)*time.Millisecond, unifiedFilter)
	consumer := NewStreamConsumer(store, infra.Redis, stream, stream+"-group", stream+"-c1",
		cfg.EventBatchSize, cfg.MaxWorkers,
		100*time.Millisecond, 1*time.Second,
		100*time.Millisecond, 5*time.Second,
		3, 5*time.Minute, 1*time.Second)
	consumer.Start(ctx)

	sharder := NewJumpHashSharder(1)
	router := NewRouter(cfg, registry, filterEngine, infra.Pool, []redis.UniversalClient{infra.Redis}, sharder, cfg.FraudStreamName, nil)
	srv := httptest.NewServer(router)

	return &adsIngestStack{
		Srv:        srv,
		Consumer:   consumer,
		Registry:   registry,
		CampaignID: campaignID,
		Stream:     stream,
		ctx:        ctx,
		Cancel:     cancel,
		cfg:        cfg,
	}
}

func (s *adsIngestStack) Close(t *testing.T) {
	t.Helper()
	s.Consumer.Close()
	_ = s.Consumer.Wait(context.Background())
	s.Cancel()
	s.Srv.Close()
}

func (s *adsIngestStack) restartConsumer(t *testing.T, infra *adsChaosInfra) {
	t.Helper()
	s.Consumer.Close()
	_ = s.Consumer.Wait(context.Background())

	store := NewPostgresStore(infra.Queries, 1*time.Second)
	s.Consumer = NewStreamConsumer(store, infra.Redis, s.Stream, s.Stream+"-group", s.Stream+"-c1",
		s.cfg.EventBatchSize, s.cfg.MaxWorkers,
		100*time.Millisecond, 1*time.Second,
		100*time.Millisecond, 5*time.Second,
		3, 5*time.Minute, 1*time.Second)
	s.Consumer.Start(s.ctx)
}

func postChaosClick(t *testing.T, srvURL string, campaignID uuid.UUID) int {
	t.Helper()
	payload := map[string]any{
		"campaign_id": campaignID,
		"type":        "click",
		"click_id":    uuid.NewString(),
		"payload":     map[string]string{"chaos": "1"},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	resp, err := http.Post(srvURL+"/track", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func countChaosCampaignEvents(t *testing.T, pool *pgxpool.Pool, campaignID uuid.UUID) int64 {
	t.Helper()
	var n int64
	err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM events WHERE campaign_id = $1", campaignID).Scan(&n)
	require.NoError(t, err)
	return n
}

func chaosDomainEventClick(campaignID uuid.UUID) *domain.Event {
	return &domain.Event{
		CampaignID: campaignID,
		Type:       "click",
		ClickID:    uuid.NewString(),
	}
}

func itoaAdsChaos(n int) string {
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

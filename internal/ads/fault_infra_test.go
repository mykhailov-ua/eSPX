package ads

import (
	"context"
	"encoding/json"
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

const (
	adsChaosRedisFastTimeout = 200 * time.Millisecond
	adsChaosRedisBreakerFail = 3
	adsChaosRedisBreakerHalf = 2
	adsChaosRedisBreakerOpen = 300 * time.Millisecond
)

// adsChaosInfra holds live Postgres and Redis for ads chaos tests.
type adsChaosInfra struct {
	Pool           *pgxpool.Pool
	Redis          redis.UniversalClient
	RedisBreaker   *database.RedisBreaker
	Queries        db.Querier
	PGContainer    *postgres.PostgresContainer
	RedisContainer testcontainers.Container
}

// adsIngestStack wires gnet tracker handler and a stream consumer against chaos infra.
type adsIngestStack struct {
	Handler       *AdsPacketHandler
	Consumer      *StreamConsumer
	Registry      *CampaignRegistry
	UnifiedFilter *UnifiedFilter
	CampaignID    uuid.UUID
	Stream        string
	ctx           context.Context
	Cancel        context.CancelFunc
	probeCancel   context.CancelFunc
	redisMetrics  bool
	cfg           *config.Config
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

	infra := &adsChaosInfra{
		Pool:           pool,
		Queries:        db.New(pool),
		PGContainer:    pgContainer,
		RedisContainer: redisContainer,
	}
	infra.Redis = infra.dialRedisClient(t, endpoint)

	cleanup := func() {
		_ = infra.Redis.Close()
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

func (infra *adsChaosInfra) dialRedisClient(t *testing.T, endpoint string) redis.UniversalClient {
	t.Helper()
	if infra.RedisBreaker == nil {
		infra.RedisBreaker = database.NewRedisBreaker(
			adsChaosRedisBreakerFail,
			adsChaosRedisBreakerHalf,
			adsChaosRedisBreakerOpen,
		)
	}
	client := redis.NewClient(&redis.Options{
		Addr:         endpoint,
		ReadTimeout:  adsChaosRedisFastTimeout,
		WriteTimeout: adsChaosRedisFastTimeout,
	})
	client.AddHook(database.NewRedisCircuitBreakerHook(infra.RedisBreaker, chaosRedisShardLabel))
	require.NoError(t, client.Ping(context.Background()).Err())
	return client
}

func (infra *adsChaosInfra) refreshRedisClient(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_ = infra.Redis.Close()
	// Fresh breaker after container recovery (matches tracker process restart semantics).
	infra.RedisBreaker = database.NewRedisBreaker(
		adsChaosRedisBreakerFail,
		adsChaosRedisBreakerHalf,
		adsChaosRedisBreakerOpen,
	)
	endpoint, err := infra.RedisContainer.Endpoint(ctx, "")
	require.NoError(t, err)
	infra.Redis = infra.dialRedisClient(t, endpoint)
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
	return startAdsIngestStackOpts(t, infra, stream, adsIngestStackOpts{filterTimeoutMs: 2000})
}

func startAdsIngestStackWithFilterTimeout(t *testing.T, infra *adsChaosInfra, stream string, filterTimeoutMs int) *adsIngestStack {
	return startAdsIngestStackOpts(t, infra, stream, adsIngestStackOpts{filterTimeoutMs: filterTimeoutMs})
}

func startAdsIngestStackWithRedisMetrics(t *testing.T, infra *adsChaosInfra, stream string) *adsIngestStack {
	return startAdsIngestStackOpts(t, infra, stream, adsIngestStackOpts{
		filterTimeoutMs: 2000,
		redisMetrics:    true,
	})
}

type adsIngestStackOpts struct {
	filterTimeoutMs int
	redisMetrics    bool
}

func startAdsIngestStackOpts(t *testing.T, infra *adsChaosInfra, stream string, opts adsIngestStackOpts) *adsIngestStack {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	cfg := &config.Config{
		EventBatchSize:     10,
		EventFlushMs:       100,
		StatsFlushMs:       100,
		MaxWorkers:         2,
		WriteTimeoutMs:     2000,
		FilterTimeoutMs:    opts.filterTimeoutMs,
		MaxRequestBodySize: 1024 * 1024,
		StreamMaxLen:       100000,
	}

	registry := newChaosRegistry(t, infra.Queries)
	sharder := NewJumpHashSharder(1)
	registry.SetBudgetWarmer(NewBudgetCacheWarmer([]redis.UniversalClient{infra.Redis}, sharder))
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

	handler := NewAdsPacketHandler(cfg, registry, filterEngine, infra.Pool, []redis.UniversalClient{infra.Redis}, sharder, cfg.FraudStreamName, nil)

	stack := &adsIngestStack{
		Handler:       handler,
		Consumer:      consumer,
		Registry:      registry,
		UnifiedFilter: unifiedFilter,
		CampaignID:    campaignID,
		Stream:        stream,
		ctx:           ctx,
		Cancel:        cancel,
		redisMetrics:  opts.redisMetrics,
		cfg:           cfg,
	}
	if opts.redisMetrics {
		stack.startRedisHealthProbe(t)
	} else {
		handler.SetHealthProbeState(true, true)
	}
	return stack
}

func (s *adsIngestStack) startRedisHealthProbe(t *testing.T) {
	t.Helper()
	if s.probeCancel != nil {
		s.probeCancel()
	}
	probeCtx, cancel := context.WithCancel(s.ctx)
	s.probeCancel = cancel
	exportHealthProbeMetrics(true, []int32{1})
	s.Handler.StartHealthProbe(probeCtx)
}

func (s *adsIngestStack) Close(t *testing.T) {
	t.Helper()
	if s.probeCancel != nil {
		s.probeCancel()
	}
	if s.Handler != nil {
		_ = s.Handler.Stop(context.Background())
	}
	s.Consumer.Close()
	_ = s.Consumer.Wait(context.Background())
	s.Cancel()
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

func postChaosClick(t *testing.T, h *AdsPacketHandler, campaignID uuid.UUID) int {
	return postChaosTrack(t, h, campaignID, "click", "chaos-user-1", uuid.NewString())
}

func postChaosImpression(t *testing.T, h *AdsPacketHandler, campaignID uuid.UUID, userID string) int {
	return postChaosTrack(t, h, campaignID, "impression", userID, uuid.NewString())
}

func postChaosTrack(t *testing.T, h *AdsPacketHandler, campaignID uuid.UUID, evtType, userID, clickID string) int {
	t.Helper()
	payload := map[string]any{
		"campaign_id": campaignID,
		"type":        evtType,
		"click_id":    clickID,
		"user_id":     userID,
		"payload":     map[string]string{"chaos": "1"},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	status, _ := PostTrackGnetJSON(h, body)
	return status
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

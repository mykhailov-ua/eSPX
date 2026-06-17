package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

type chaosInfra struct {
	Pool           *pgxpool.Pool
	Redis          redis.UniversalClient
	PGContainer    testcontainers.Container
	RedisContainer testcontainers.Container
}

// setupChaosInfra boots testcontainers and returns handles for real dependency fault injection.
func setupChaosInfra(t *testing.T) (*chaosInfra, func()) {
	t.Helper()
	pgC, pool, cleanupPG := setupTestDBContainer(t)
	redisC, rdb, cleanupRedis := setupTestRedisContainer(t)
	return &chaosInfra{
			Pool:           pool,
			Redis:          rdb,
			PGContainer:    pgC,
			RedisContainer: redisC,
		}, func() {
			cleanupRedis()
			cleanupPG()
		}
}

type ingestStack struct {
	Srv        *httptest.Server
	Consumer   *ads.StreamConsumer
	CampaignID uuid.UUID
	Stream     string
	Cancel     context.CancelFunc
}

// startIngestStack wires tracker router and stream consumer against live PG and Redis.
func startIngestStack(t *testing.T, pool *pgxpool.Pool, rdb redis.UniversalClient, stream string) *ingestStack {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	queries := db.New(pool)
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

	pm := database.NewPartitionManager(pool, 7, 1)
	require.NoError(t, pm.Run(ctx))

	customerID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Chaos Customer", 1_000_000_000)
	require.NoError(t, err)

	campaignID := uuid.New()
	_, err = pool.Exec(ctx, "INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
		campaignID, "Chaos Campaign", "ACTIVE", customerID, 100_000_000)
	require.NoError(t, err)

	registry := newTestRegistry(t, queries)
	_, _ = registry.Sync(ctx)

	store := ads.NewPostgresStore(queries, 1*time.Second)
	campaignRepo := ads.NewCampaignRepo(queries)
	unifiedFilter := ads.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		ads.NewJumpHashSharder(1),
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
	filterEngine := ads.NewFilterEngine(time.Duration(cfg.FilterTimeoutMs)*time.Millisecond, unifiedFilter)
	consumer := ads.NewStreamConsumer(store, rdb, stream, stream+"-group", stream+"-c1",
		cfg.EventBatchSize, cfg.MaxWorkers,
		100*time.Millisecond, 1*time.Second,
		100*time.Millisecond, 5*time.Second,
		3, 5*time.Minute, 1*time.Second)
	consumer.Start(ctx)

	sharder := ads.NewJumpHashSharder(1)
	router := ads.NewRouter(cfg, registry, filterEngine, pool, []redis.UniversalClient{rdb}, sharder, cfg.FraudStreamName, nil)
	srv := httptest.NewServer(router)

	return &ingestStack{
		Srv:        srv,
		Consumer:   consumer,
		CampaignID: campaignID,
		Stream:     stream,
		Cancel:     cancel,
	}
}

func (s *ingestStack) Close(t *testing.T) {
	t.Helper()
	s.Consumer.Close()
	_ = s.Consumer.Wait(context.Background())
	s.Cancel()
	s.Srv.Close()
}

func postClick(t *testing.T, srvURL string, campaignID uuid.UUID) int {
	t.Helper()
	payload := map[string]any{
		"campaign_id": campaignID,
		"type":        "click",
		"payload":     map[string]string{"chaos": "1"},
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(srvURL+"/track", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func countCampaignEvents(t *testing.T, pool *pgxpool.Pool, campaignID uuid.UUID) int64 {
	t.Helper()
	var n int64
	err := pool.QueryRow(context.Background(), "SELECT count(*) FROM events WHERE campaign_id = $1", campaignID).Scan(&n)
	require.NoError(t, err)
	return n
}

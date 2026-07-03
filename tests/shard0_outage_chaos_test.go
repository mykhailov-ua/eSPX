package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/management"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_Shard0Outage automates CHAOS.md §6 scenario A: shard 0 down must not
// stop tracking on shards 1–3; outbox stays PENDING until recovery.
func TestChaos_Shard0Outage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const numShards = 4

	pool, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	shardInfra := setupTestRedisShardsChaos(t, numShards)
	rdbs := shardInfra.UniversalClients()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	sharder := ads.NewStaticSlotSharder(numShards)
	campaignIDs := make([]uuid.UUID, numShards)
	for i := range campaignIDs {
		campaignIDs[i] = campaignIDForShard(t, sharder, i)
	}

	customerID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Shard0 Chaos Customer", 1_000_000_000)
	require.NoError(t, err)

	for _, campaignID := range campaignIDs {
		_, err = pool.Exec(ctx,
			"INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
			campaignID, "Shard0 Campaign", "ACTIVE", customerID, 100_000_000,
		)
		require.NoError(t, err)
	}

	registry := newTestRegistry(t, queries)
	registry.SetBudgetWarmer(ads.NewBudgetCacheWarmer(rdbs, sharder))
	_, err = registry.Sync(ctx)
	require.NoError(t, err)

	cfg := &config.Config{
		EventBatchSize:        10,
		EventFlushMs:          100,
		MaxWorkers:            2,
		WriteTimeoutMs:        1000,
		FilterTimeoutMs:       500,
		MaxRequestBodySize:    1024 * 1024,
		StreamMaxLen:          100000,
		CampaignUpdateChannel: "campaigns:shard0-chaos",
	}

	partManager := database.NewPartitionManager(pool, 7, 2)
	require.NoError(t, partManager.Run(ctx))

	campaignRepo := ads.NewCampaignRepo(queries)
	unifiedFilter := ads.NewUnifiedFilter(
		rdbs,
		sharder,
		registry,
		campaignRepo,
		1000,
		time.Minute,
		45*time.Second,
		24*time.Hour,
		100_000,
		10_000,
		"shard0-chaos-stream",
		100000,
	)
	filterEngine := ads.NewFilterEngine(time.Duration(cfg.FilterTimeoutMs)*time.Millisecond, unifiedFilter)
	handler := ads.NewAdsPacketHandler(cfg, registry, filterEngine, pool, rdbs, sharder, cfg.FraudStreamName, nil)
	defer handler.Stop(ctx)

	for i, campaignID := range campaignIDs {
		status, _ := postClickCampaign(t, handler, campaignID, uuid.NewString())
		require.Equal(t, http.StatusAccepted, status, "baseline shard %d", i)
	}

	statusBaseline, baselineLatency := postClickCampaign(t, handler, campaignIDs[1], uuid.NewString())
	require.Equal(t, http.StatusAccepted, statusBaseline)

	svc := management.NewService(pool, rdbs, sharder, cfg)
	defer svc.Close()

	stopRedisShardContainer(t, shardInfra.Containers[0])
	require.Eventually(t, func() bool {
		return shardInfra.Clients[0].Ping(ctx).Err() != nil
	}, 15*time.Second, 100*time.Millisecond, "shard 0 must be unreachable after stop")

	tripRedisBreaker(t, shardInfra.Clients[0], shardInfra.Breakers[0])

	statusShard0, _ := postClickCampaign(t, handler, campaignIDs[0], uuid.NewString())
	assert.NotEqual(t, http.StatusAccepted, statusShard0, "shard 0 campaign must not accept while redis-0 is down")
	assert.True(t, statusShard0 == http.StatusServiceUnavailable || statusShard0 == http.StatusInternalServerError,
		"shard 0 expected 503 or 500, got %d", statusShard0)

	budgetLimit := latencyBudget(baselineLatency)
	for shard := 1; shard < numShards; shard++ {
		status, elapsed := postClickCampaign(t, handler, campaignIDs[shard], uuid.NewString())
		assert.Equal(t, http.StatusAccepted, status, "shard %d must keep accepting", shard)
		assert.LessOrEqual(t, elapsed, budgetLimit, "shard %d latency regression", shard)
	}

	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"rate_limit_per_min": "199"}))
	eventID := latestOutboxEventID(t, pool, "UPDATE_SETTINGS")

	outboxWorker := management.NewOutboxWorker(svc)
	processed, err := outboxWorker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, processed)
	assert.Equal(t, "PENDING", outboxStatus(t, pool, eventID))

	startRedisShardContainer(t, shardInfra.Containers[0])
	waitRedisContainerReady(t, shardInfra.Containers[0])
	shardInfra.replaceShardClient(t, 0, rdbs)
	waitRedisBreakerClosed(t, shardInfra.Clients[0], shardInfra.Breakers[0])

	require.NoError(t, outboxWorker.ProcessOutbox(ctx))
	assert.Equal(t, "PROCESSED", outboxStatus(t, pool, eventID))

	statusRecovered, _ := postClickCampaign(t, handler, campaignIDs[0], uuid.NewString())
	require.Equal(t, http.StatusAccepted, statusRecovered, "shard 0 track must recover after redis-0 restart")

	for shard := 1; shard < numShards; shard++ {
		budgetKey := "budget:campaign:" + campaignIDs[shard].String()
		remaining, err := shardInfra.Clients[shard].Get(ctx, budgetKey).Int64()
		require.NoError(t, err)
		assert.GreaterOrEqual(t, remaining, int64(0), "budget must stay non-negative on shard %d", shard)
	}

	logChaosProof(t, "shard_0_outage", map[string]string{
		"status":        "recovered",
		"shards_123_ok": "true",
		"outbox":        "processed",
	})
}

func postClickCampaign(t *testing.T, h *ads.AdsPacketHandler, campaignID uuid.UUID, clickID string) (int, time.Duration) {
	t.Helper()
	start := time.Now()
	payload := map[string]any{
		"campaign_id": campaignID,
		"type":        "click",
		"click_id":    clickID,
		"payload":     map[string]string{"chaos": "shard0"},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	status, _ := ads.PostTrackGnetJSON(h, body)
	return status, time.Since(start)
}

func latestOutboxEventID(t *testing.T, pool *pgxpool.Pool, eventType string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(),
		`SELECT id FROM outbox_events WHERE event_type = $1 ORDER BY id DESC LIMIT 1`, eventType).Scan(&id)
	require.NoError(t, err)
	return id
}

func outboxStatus(t *testing.T, pool *pgxpool.Pool, eventID int64) string {
	t.Helper()
	var status string
	err := pool.QueryRow(context.Background(),
		`SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&status)
	require.NoError(t, err)
	return status
}

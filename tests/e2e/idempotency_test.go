// Package e2e_test exercises the full ingest path from HTTP accept through Redis
// filters, stream consumers, and Postgres persistence. Tests use testcontainers
// and run only when the -short flag is not set.
package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"espx/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const e2eClickAmountMicro = 100_000

// TestE2E_Idempotency implements CHAOS.md section 4.3. A duplicate click_id
// replay returns HTTP 202 without debiting budget again, appending a second
// stream entry, or inserting a second events row. SyncWorker retries must not
// add duplicate sync_idempotency rows.
func TestE2E_Idempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := testutil.SetupAdsPostgres(t)
	defer cleanupDB()

	rdb, cleanupRedis := testutil.SetupRedis(t)
	defer cleanupRedis()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	cfg := &config.Config{
		ClickAmount:        e2eClickAmountMicro,
		EventBatchSize:     10,
		EventFlushMs:       100,
		StatsFlushMs:       100,
		MaxWorkers:         2,
		WriteTimeoutMs:     1000,
		FilterTimeoutMs:    1000,
		MaxRequestBodySize: 1024 * 1024,
		StreamMaxLen:       100000,
	}

	partManager := database.NewPartitionManager(pool, 7, 2)
	require.NoError(t, partManager.Run(ctx))

	sharder := ingestion.NewStaticSlotSharder(1)
	customerID := uuid.New()
	campaignID := uuid.New()
	clickID := uuid.NewString()

	const budgetLimitMicro = 100_000_000
	_, err := pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Idempotency Customer", budgetLimitMicro)
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		"INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
		campaignID, "Idempotency Campaign", "ACTIVE", customerID, budgetLimitMicro,
	)
	require.NoError(t, err)

	registry := testutil.NewAdsRegistry(t, queries)
	budgetWarmer := ingestion.NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, sharder)
	registry.SetBudgetWarmer(budgetWarmer)
	_, err = registry.Sync(ctx)
	require.NoError(t, err)
	_, err = budgetWarmer.WarmFromRegistry(ctx, registry)
	require.NoError(t, err)

	budgetKey := "budget:campaign:" + campaignID.String()
	initialBudget, err := rdb.Get(ctx, budgetKey).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(budgetLimitMicro), initialBudget)

	store := ingestion.NewPostgresStore(queries, 1*time.Second)
	campaignRepo := ingestion.NewCampaignRepoWithDB(pool, queries)
	customerRepo := ingestion.NewCustomerRepoWithDB(pool, queries)
	streamName := "test-idempotency-stream"

	unifiedFilter := ingestion.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		sharder,
		registry,
		campaignRepo,
		1000,
		time.Minute,
		45*time.Second,
		24*time.Hour,
		e2eClickAmountMicro,
		10_000,
		streamName,
		100000,
	)
	filterEngine := ingestion.NewFilterEngine(time.Duration(cfg.FilterTimeoutMs)*time.Millisecond, unifiedFilter)

	consumer := ingestion.NewStreamConsumer(
		store, rdb, streamName,
		"test-idempotency-group", "test-idempotency-c1",
		cfg.EventBatchSize, cfg.MaxWorkers,
		100*time.Millisecond, 1*time.Second,
		100*time.Millisecond, 5*time.Second,
		5, 5*time.Minute, 1*time.Second,
	)
	consumer.Start(ctx)
	defer func() {
		consumer.Close()
		_ = consumer.Wait(context.Background())
	}()

	handler := ingestion.NewAdsPacketHandler(cfg, registry, filterEngine, pool, []redis.UniversalClient{rdb}, sharder, cfg.FraudStreamName, nil)
	defer handler.Stop(ctx)

	payload := map[string]any{
		"campaign_id": campaignID,
		"type":        "click",
		"click_id":    clickID,
		"payload":     map[string]string{"case": "idempotency"},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	status1, _ := ingestion.PostTrackGnetJSON(handler, body)
	assert.Equal(t, http.StatusAccepted, status1)

	afterFirst, err := rdb.Get(ctx, budgetKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, initialBudget-e2eClickAmountMicro, afterFirst)

	status2, _ := ingestion.PostTrackGnetJSON(handler, body)
	assert.Equal(t, http.StatusAccepted, status2, "idempotent replay must still accept")

	afterSecond, err := rdb.Get(ctx, budgetKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, afterFirst, afterSecond, "budget must not be debited twice")

	xlen, err := rdb.XLen(ctx, streamName).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), xlen, "only one stream entry for duplicate click_id")

	idemKey := "idempotency:click:" + clickID
	exists, err := rdb.Exists(ctx, idemKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	assert.Eventually(t, func() bool {
		var eventCount int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE click_id = $1", clickID).Scan(&eventCount)
		return err == nil && eventCount == 1
	}, 5*time.Second, 100*time.Millisecond)

	assert.Eventually(t, func() bool {
		var clicks int64
		err := pool.QueryRow(ctx, "SELECT clicks_count FROM campaign_stats WHERE campaign_id = $1", campaignID).Scan(&clicks)
		return err == nil && clicks == 1
	}, 5*time.Second, 100*time.Millisecond)

	syncWorker := ingestion.NewSyncWorker(rdb, campaignRepo, customerRepo, time.Hour, nil, 0)
	syncWorker.SyncAll(ctx)

	var syncIdemAfterFirst int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM sync_idempotency").Scan(&syncIdemAfterFirst)
	require.NoError(t, err)
	require.GreaterOrEqual(t, syncIdemAfterFirst, 1)

	syncWorker.SyncAll(ctx)

	var currentSpend int64
	err = pool.QueryRow(ctx, "SELECT current_spend FROM campaigns WHERE id = $1", campaignID).Scan(&currentSpend)
	require.NoError(t, err)
	assert.Equal(t, int64(e2eClickAmountMicro), currentSpend)

	var syncIdemAfterSecond int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM sync_idempotency").Scan(&syncIdemAfterSecond)
	require.NoError(t, err)
	assert.Equal(t, syncIdemAfterFirst, syncIdemAfterSecond, "retry SyncAll must not add sync_idempotency rows")
}

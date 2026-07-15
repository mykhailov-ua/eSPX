package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"
	"github.com/google/uuid"

	"espx/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const multishardCount = 4

// TestE2E_Multishard exercises the full ingest chain on production topology:
// four standalone Redis masters and StaticSlotSharder routing.
func TestE2E_Multishard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := testutil.SetupAdsPostgres(t)
	defer cleanupDB()

	rdbs := testutil.SetupRedisShards(t, multishardCount)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	cfg := &config.Config{
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

	sharder := ingestion.NewStaticSlotSharder(multishardCount)
	campaignIDs := make([]uuid.UUID, multishardCount)
	for i := range campaignIDs {
		campaignIDs[i] = testutil.CampaignIDForShard(t, sharder, i)
		assert.Equal(t, i, sharder.GetShard(campaignIDs[i]), "campaign must map to shard %d", i)
	}

	customerID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Multishard Customer", 1_000_000_000)
	require.NoError(t, err)

	for i, campaignID := range campaignIDs {
		_, err = pool.Exec(ctx,
			"INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
			campaignID, fmt.Sprintf("Multishard Campaign %d", i), "ACTIVE", customerID, 100_000_000,
		)
		require.NoError(t, err)
	}

	registry := testutil.NewAdsRegistry(t, queries)
	budgetWarmer := ingestion.NewBudgetCacheWarmer(rdbs, sharder)
	registry.SetBudgetWarmer(budgetWarmer)
	_, err = registry.Sync(ctx)
	require.NoError(t, err)
	_, err = budgetWarmer.WarmFromRegistry(ctx, registry)
	require.NoError(t, err)

	store := ingestion.NewPostgresStore(queries, 1*time.Second)
	campaignRepo := ingestion.NewCampaignRepo(queries)
	streamName := "test-multishard-stream"

	unifiedFilter := ingestion.NewUnifiedFilter(
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
		streamName,
		100000,
	)
	filterEngine := ingestion.NewFilterEngine(time.Duration(cfg.FilterTimeoutMs)*time.Millisecond, unifiedFilter)

	handler := ingestion.NewAdsPacketHandler(cfg, registry, filterEngine, pool, rdbs, sharder, cfg.FraudStreamName, nil)
	defer handler.Stop(ctx)

	for i, campaignID := range campaignIDs {
		payload := map[string]any{
			"campaign_id": campaignID,
			"type":        "click",
			"click_id":    uuid.NewString(),
			"payload":     map[string]string{"shard": fmt.Sprintf("%d", i)},
		}
		body, err := json.Marshal(payload)
		require.NoError(t, err)

		status, _ := ingestion.PostTrackGnetJSON(handler, body)
		assert.Equal(t, http.StatusAccepted, status, "shard %d track", i)
	}

	for _, campaignID := range campaignIDs {
		expectedShard := sharder.GetShard(campaignID)
		budgetKey := "budget:campaign:" + campaignID.String()

		for shardID, rdb := range rdbs {
			exists, err := rdb.Exists(ctx, budgetKey).Result()
			require.NoError(t, err)
			if shardID == expectedShard {
				assert.Equal(t, int64(1), exists, "budget key must exist on shard %d for campaign %s", shardID, campaignID)
			} else {
				assert.Equal(t, int64(0), exists, "budget key must not exist on shard %d for campaign %s", shardID, campaignID)
			}
		}
	}

	for shardID, rdb := range rdbs {
		xlen, err := rdb.XLen(ctx, streamName).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), xlen, "shard %d must have exactly one stream entry", shardID)
	}

	consumers := make([]*ingestion.StreamConsumer, multishardCount)
	for i, rdb := range rdbs {
		consumers[i] = ingestion.NewStreamConsumer(
			store, rdb, streamName,
			"test-multishard-group", fmt.Sprintf("test-multishard-c%d", i),
			cfg.EventBatchSize, cfg.MaxWorkers,
			100*time.Millisecond, 1*time.Second,
			100*time.Millisecond, 5*time.Second,
			5, 5*time.Minute, 1*time.Second,
		)
		consumers[i].Start(ctx)
	}
	defer func() {
		for _, c := range consumers {
			c.Close()
			_ = c.Wait(context.Background())
		}
	}()

	for _, campaignID := range campaignIDs {
		campID := campaignID
		assert.Eventually(t, func() bool {
			var clicks int64
			err := pool.QueryRow(ctx, "SELECT clicks_count FROM campaign_stats WHERE campaign_id = $1", campID).Scan(&clicks)
			return err == nil && clicks == 1
		}, 5*time.Second, 100*time.Millisecond, "campaign_stats for %s", campID)

		assert.Eventually(t, func() bool {
			var eventCount int
			err := pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE campaign_id = $1", campID).Scan(&eventCount)
			return err == nil && eventCount == 1
		}, 5*time.Second, 100*time.Millisecond, "events row for %s", campID)
	}
}

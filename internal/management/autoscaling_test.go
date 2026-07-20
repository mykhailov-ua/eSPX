package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSmartBudgetAutoscaling guards CTR-based budget shifts from low to high performers without corrupting sync state.
func TestSmartBudgetAutoscaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)

	rdb, cleanupRedis := database.SetupTestRedis(t)

	cfg := &config.Config{
		CampaignUpdateChannel:       "test:autoscaling-updates",
		AutoscaleHighCTRThreshold:   0.015,
		AutoscaleMinImpressions:     100,
		AutoscaleLowCTRThreshold:    0.005,
		AutoscaleMinRemainingBudget: 20_000_000,
		AutoscaleShiftAmount:        10_000_000,
	}
	cfg.Lifecycle.WaitTimeoutMs = 500

	sharder := ingestion.NewJumpHashSharder(1)
	svc := NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)

	t.Cleanup(func() {
		svc.Close()
		cleanupRedis()
		cleanupDB()
	})

	ctx := context.Background()
	customerID := uuid.New()

	err := svc.CreateCustomer(ctx, customerID, "Smart Customer", 1_000_000_000, "USD")
	require.NoError(t, err)

	campaignA, err := svc.CreateCampaign(ctx, testCampaignSpec(customerID, "Low CTR Campaign", 100_000_000, "low-idem"))
	require.NoError(t, err)

	campaignB, err := svc.CreateCampaign(ctx, testCampaignSpec(customerID, "High CTR Campaign", 100_000_000, "high-idem"))
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		"INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count) VALUES ($1, CURRENT_DATE, 1000, 2, 0)",
		ingestion.ToUUID(campaignA),
	)
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		"INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count) VALUES ($1, CURRENT_DATE, 500, 15, 0)",
		ingestion.ToUUID(campaignB),
	)
	require.NoError(t, err)

	err = rdb.SAdd(ctx, "budget:dirty_campaigns", campaignA.String()).Err()
	require.NoError(t, err)
	err = rdb.Set(ctx, "budget:sync:campaign:"+campaignA.String(), 5000000, 0).Err()
	require.NoError(t, err)

	queries := db.New(pool)
	campaignRepo := ingestion.NewCampaignRepo(queries)
	customerRepo := ingestion.NewCustomerRepo(queries)
	syncWorker := ingestion.NewSyncWorker(rdb, campaignRepo, customerRepo, 100*time.Millisecond, 0, nil, 0)

	err = rdb.Set(ctx, "budget:campaign:"+campaignA.String(), 100000000, 0).Err()
	require.NoError(t, err)
	err = rdb.Set(ctx, "budget:campaign:"+campaignB.String(), 100000000, 0).Err()
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)

	var balanceBefore int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT balance FROM customers WHERE id = $1`, ingestion.ToUUID(customerID)).Scan(&balanceBefore))

	err = svc.AutoscaleBudgets(ctx, []*ingestion.SyncWorker{syncWorker})
	require.NoError(t, err)

	var spendA string
	err = pool.QueryRow(ctx, "SELECT current_spend::TEXT FROM campaigns WHERE id = $1", ingestion.ToUUID(campaignA)).Scan(&spendA)
	require.NoError(t, err)
	assert.Equal(t, "5000000", spendA)

	var limitA string
	err = pool.QueryRow(ctx, "SELECT budget_limit::TEXT FROM campaigns WHERE id = $1", ingestion.ToUUID(campaignA)).Scan(&limitA)
	require.NoError(t, err)
	assert.Equal(t, "90000000", limitA)

	var limitB string
	err = pool.QueryRow(ctx, "SELECT budget_limit::TEXT FROM campaigns WHERE id = $1", ingestion.ToUUID(campaignB)).Scan(&limitB)
	require.NoError(t, err)
	assert.Equal(t, "110000000", limitB)

	var outboxCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'CREATE_CAMPAIGN'").Scan(&outboxCount)
	require.NoError(t, err)
	assert.Equal(t, 2, outboxCount)

	val, err := rdb.Get(ctx, "budget:sync:campaign:"+campaignA.String()).Result()
	assert.Equal(t, redis.Nil, err)
	assert.Empty(t, val)

	var autoscaleFreeze, autoscaleRelease int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM balance_ledger
		WHERE type = 'FREEZE' AND idempotency_hash LIKE 'autoscale-transfer:%'`).Scan(&autoscaleFreeze))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM balance_ledger
		WHERE type = 'RELEASE' AND idempotency_hash LIKE 'autoscale-transfer:%'`).Scan(&autoscaleRelease))
	assert.Equal(t, 1, autoscaleFreeze)
	assert.Equal(t, 1, autoscaleRelease)

	var balanceAfter int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT balance FROM customers WHERE id = $1`, ingestion.ToUUID(customerID)).Scan(&balanceAfter))
	assert.Equal(t, balanceBefore, balanceAfter)
}

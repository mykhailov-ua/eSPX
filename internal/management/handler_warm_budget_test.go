package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWarmCampaignBudgetAPI(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Warm Co", 1_000_000_000, "USD"))

	budget := int64(100_000_000)
	spend := int64(25_000_000)
	campaignID, err := svc.CreateCampaign(ctx, testCampaignSpec(customerID, "Warm Camp", budget, "warm-idem"))
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = $1 WHERE id = $2", spend, campaignID)
	require.NoError(t, err)
	_, err = rdb.Del(ctx, "budget:campaign:"+campaignID.String()).Result()
	require.NoError(t, err)

	req, _ := http.NewRequest("POST", "/admin/campaigns/"+campaignID.String()+"/warm-budget", nil)
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	val, err := rdb.Get(ctx, "budget:campaign:"+campaignID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, budget-spend, val)
}

func TestWarmCampaignBudget_noOutboxOrPubsub(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	channel := "campaigns:warm-budget-test"
	cfg := &config.Config{AdminAPIKey: "test-secret", CampaignUpdateChannel: channel}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()
	sub := rdb.Subscribe(ctx, channel)
	defer sub.Close()
	_, err := sub.Receive(ctx)
	require.NoError(t, err)

	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "No Pub Co", 1_000_000_000, "USD"))
	campaignID, err := svc.CreateCampaign(ctx, testCampaignSpec(customerID, "No Pub", 50_000_000, "warm-nopub"))
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)
	_, err = rdb.Del(ctx, "budget:campaign:"+campaignID.String()).Result()
	require.NoError(t, err)

	req, _ := http.NewRequest("POST", "/admin/campaigns/"+campaignID.String()+"/warm-budget", nil)
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events`).Scan(&outboxCount))
	assert.Equal(t, 0, outboxCount)

	_, err = sub.ReceiveTimeout(ctx, 200*time.Millisecond)
	assert.Error(t, err, "warm-budget must not publish campaigns:update")
}

func TestWarmBudget_ExplainAnalyze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping warm-budget EXPLAIN ANALYZE in short mode")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	ctx := context.Background()

	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Explain Warm", 1_000_000_000, "USD"))
	campaignID, err := svc.CreateCampaign(ctx, testCampaignSpec(customerID, "Explain Camp", 100_000_000, "explain-warm"))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE campaigns SET current_spend = $1 WHERE id = $2`, int64(25_000_000), campaignID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ANALYZE campaigns`)
	require.NoError(t, err)

	queries := []struct {
		name string
		sql  string
	}{
		{
			name: "remaining_budget_lookup",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT budget_limit, current_spend FROM campaigns WHERE id = $1`,
		},
		{
			name: "remaining_budget_computed",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT GREATEST(budget_limit - current_spend, 0) AS remaining_micro FROM campaigns WHERE id = $1`,
		},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			rows, err := pool.Query(ctx, q.sql, campaignID)
			require.NoError(t, err)
			defer rows.Close()

			var plan []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				plan = append(plan, line)
			}
			require.NoError(t, rows.Err())
			require.NotEmpty(t, plan)
			t.Logf("plan %s:\n%s", q.name, joinPlan(plan))
		})
	}
}

func TestOptimizeBidFloors_writesRedis(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		BidFloorWinRateLow:  0.05,
		BidFloorWinRateHigh: 0.25,
		BidFloorAdjustPct:   10,
		BidFloorMinMicro:    1000,
	}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)

	ctx := context.Background()
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Floor Co", 1_000_000, "USD"))
	_, err := svc.CreateRtbDeal(ctx, RtbDealCreateSpec{
		DealID:     "opt-deal-1",
		FloorMicro: 200_000,
		CustomerID: customerID.String(),
	})
	require.NoError(t, err)

	recs, err := svc.OptimizeBidFloors(ctx)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, int64(200_000), recs[0].RecommendedMicro)

	val, err := rdb.Get(ctx, ingestion.RtbFloorRedisKeyPrefix+"opt-deal-1").Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(200_000), val)
}

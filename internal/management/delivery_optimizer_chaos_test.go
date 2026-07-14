package management

import (
	"context"
	"encoding/json"
	"testing"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_DeliveryOptimizerSingleWriter proves at most one outbox event per campaign per optimizer tick (M5.0).
func TestChaos_DeliveryOptimizerSingleWriter(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel:       "test:delivery-opt",
		PacingToleranceMargin:       0.05,
		AutoscaleHighCTRThreshold:   0.02,
		AutoscaleLowCTRThreshold:    0.01,
		AutoscaleMinImpressions:     10,
		AutoscaleMinRemainingBudget: 1_000_000,
		AutoscaleShiftAmount:        5_000_000,
		MABMinImpressions:           1000,
	}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)

	ctx := context.Background()
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Opt", 500_000_000, "USD"))

	lowSpec := testCampaignSpec(customerID, "Low", 100_000_000, "opt-low")
	lowSpec.PacingMode = "EVEN"
	lowID, err := svc.CreateCampaign(ctx, lowSpec)
	require.NoError(t, err)

	highSpec := testCampaignSpec(customerID, "High", 100_000_000, "opt-high")
	highID, err := svc.CreateCampaign(ctx, highSpec)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count)
		VALUES ($1, CURRENT_DATE, 1000, 5), ($2, CURRENT_DATE, 1000, 30)
		ON CONFLICT (campaign_id, date) DO UPDATE
		SET impressions_count = EXCLUDED.impressions_count, clicks_count = EXCLUDED.clicks_count`,
		ads.ToUUID(lowID), ads.ToUUID(highID))
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `UPDATE campaigns SET current_spend = daily_budget / 10, pacing_mode = 'EVEN' WHERE id = $1`, ads.ToUUID(lowID))
	require.NoError(t, err)

	var maxIDBefore int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM outbox_events`).Scan(&maxIDBefore))

	syncWorker := ads.NewSyncWorker(rdb, ads.NewCampaignRepo(db.New(pool)), ads.NewCustomerRepo(db.New(pool)), 0)
	require.NoError(t, svc.RunDeliveryOptimizerTick(ctx, []*ads.SyncWorker{syncWorker}, false))

	rows, err := pool.Query(ctx, `SELECT event_type, payload FROM outbox_events WHERE id > $1 ORDER BY id`, maxIDBefore)
	require.NoError(t, err)
	defer rows.Close()

	perCampaign := make(map[string]int)
	for rows.Next() {
		var eventType string
		var payload []byte
		require.NoError(t, rows.Scan(&eventType, &payload))
		var body map[string]any
		require.NoError(t, json.Unmarshal(payload, &body))
		if cid, ok := body["campaign_id"].(string); ok && cid != "" {
			perCampaign[cid]++
		}
	}
	for campID, count := range perCampaign {
		assert.LessOrEqual(t, count, 1, "campaign %s emitted %d outbox events in one tick", campID, count)
	}
}

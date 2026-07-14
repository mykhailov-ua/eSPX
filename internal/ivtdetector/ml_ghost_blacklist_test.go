package ivtdetector

import (
	"context"
	"testing"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/management"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMLGhostAndBlacklist_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx := context.Background()

	// 1. Create a campaign in Postgres
	campaignID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, status, budget_limit, fraud_threshold_pass, fraud_threshold_suspect, fraud_threshold_block, ghost_ivt_enabled)
		VALUES ($1, 'Test Campaign', 'ACTIVE', 1000000000, 20, 50, 90, true)
	`, campaignID)
	require.NoError(t, err)

	// Create a Management Service
	cfg := &config.Config{}
	sharder := ads.NewStaticSlotSharder(1)
	svc := management.NewService(pool, []redis.UniversalClient{rdb}, sharder, cfg)

	// Create an OutboxWorker
	worker := management.NewOutboxWorker(svc)

	// Test ML_GHOST_IVT outbox event
	err = svc.EnqueueMLThreat(ctx, management.MLThreatPayload{
		Action:     "ghost",
		CampaignID: campaignID.String(),
		IP:         "1.1.1.1",
		Score:      75.0,
		TTLSeconds: 300,
	})
	require.NoError(t, err)

	// Process outbox events
	processed, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Greater(t, processed, 0)

	// Verify that ghost_ivt_enabled was updated to true in Postgres (it was already true, but let's set it to false first and verify)
	_, err = pool.Exec(ctx, "UPDATE campaigns SET ghost_ivt_enabled = FALSE WHERE id = $1", campaignID)
	require.NoError(t, err)

	err = svc.EnqueueMLThreat(ctx, management.MLThreatPayload{
		Action:     "ghost",
		CampaignID: campaignID.String(),
		IP:         "1.1.1.1",
		Score:      75.0,
		TTLSeconds: 300,
	})
	require.NoError(t, err)

	processed, err = worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Greater(t, processed, 0)

	var ghostEnabled bool
	err = pool.QueryRow(ctx, "SELECT ghost_ivt_enabled FROM campaigns WHERE id = $1", campaignID).Scan(&ghostEnabled)
	require.NoError(t, err)
	assert.True(t, ghostEnabled)

	// Test ML_BLACKLIST_ADD outbox event
	err = svc.EnqueueMLThreat(ctx, management.MLThreatPayload{
		Action:     "blacklist",
		CampaignID: campaignID.String(),
		IP:         "9.9.9.9",
		Score:      95.0,
		TTLSeconds: 3600,
	})
	require.NoError(t, err)

	processed, err = worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Greater(t, processed, 0)

	// Verify IP is blacklisted in Postgres
	var exists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ip_blacklist WHERE ip = '9.9.9.9')").Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)

	// Test ApplyMLOverride to clear boost
	err = svc.ApplyMLOverride(ctx, management.MLOverrideRequest{
		CampaignID: ptr(campaignID.String()),
	})
	require.NoError(t, err)

	processed, err = worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Greater(t, processed, 0)

	// Test ApplyMLOverride to remove false positive (unblock IP)
	err = svc.ApplyMLOverride(ctx, management.MLOverrideRequest{
		IP: ptr("9.9.9.9"),
	})
	require.NoError(t, err)

	processed, err = worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Greater(t, processed, 0)

	// Verify IP is no longer blacklisted in Postgres
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ip_blacklist WHERE ip = '9.9.9.9')").Scan(&exists)
	require.NoError(t, err)
	assert.False(t, exists)
}

func ptr[T any](v T) *T {
	return &v
}

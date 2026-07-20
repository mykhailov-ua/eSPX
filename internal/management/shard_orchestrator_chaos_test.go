package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestChaos_SO_NoFalseMigrate (SO-01) verifies that the ShardOrchestrator does not trigger
// a migration if the capacity scores are below the threshold.
func TestChaos_SO_NoFalseMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb0, cleanup0 := database.SetupTestRedis(t)
	defer cleanup0()
	rdb1, cleanup1 := database.SetupTestRedis(t)
	defer cleanup1()

	cfg := &config.Config{SlotMigrationEnabled: false}
	svc := NewService(pool, []redis.UniversalClient{rdb0, rdb1}, ingestion.NewStaticSlotSharder(2), cfg)
	defer svc.Close()

	// Seed campaign
	var campID uuid.UUID
	for {
		campID = uuid.New()
		if ingestion.CampaignSlotIndex(campID)%2 == 0 {
			break
		}
	}
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "SO Cust 1", 1_000_000, "USD"))

	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'so-test-1', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	// Healthy metrics
	provider := &mockShardMetricsProvider{
		metrics: map[int16]ShardMetrics{
			0: {ShardID: 0, CPUUsage: 40.0, MemoryPct: 30.0, OpsPerSec: 10000},
			1: {ShardID: 1, CPUUsage: 10.0, MemoryPct: 15.0, OpsPerSec: 1000},
		},
	}

	orchestrator := NewShardOrchestrator(svc, provider, 100*time.Millisecond)
	orchestrator.scaleThreshold = 0.85
	orchestrator.overloadLimit = 10 * time.Millisecond

	// Run multiple ticks
	for i := 0; i < 5; i++ {
		orchestrator.tick(ctx)
		time.Sleep(10 * time.Millisecond)
	}

	// Verify no migration occurred
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM campaign_shard_assignment").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	logChaosProof(t, "orchestrator_no_false_migrate", map[string]string{
		"subsystem":     "shard_orchestrator",
		"max_ema":       "0.40",
		"threshold":     "0.85",
		"false_migrate": "false",
	})
}

// TestChaos_SO_CampaignRoutingMigration (SO-02) verifies that the ShardOrchestrator
// triggers a migration when a shard is consistently overloaded.
func TestChaos_SO_CampaignRoutingMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb0, cleanup0 := database.SetupTestRedis(t)
	defer cleanup0()
	rdb1, cleanup1 := database.SetupTestRedis(t)
	defer cleanup1()

	cfg := &config.Config{SlotMigrationEnabled: false}
	svc := NewService(pool, []redis.UniversalClient{rdb0, rdb1}, ingestion.NewStaticSlotSharder(2), cfg)
	defer svc.Close()

	// Seed campaign
	var campID uuid.UUID
	for {
		campID = uuid.New()
		if ingestion.CampaignSlotIndex(campID)%2 == 0 {
			break
		}
	}
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "SO Cust 2", 1_000_000, "USD"))

	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'so-test-2', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	// Seed keys on source shard
	key := "budget:campaign:" + campID.String()
	require.NoError(t, rdb0.Set(ctx, key, "850000", 0).Err())

	// Overloaded metrics on Shard 0
	provider := &mockShardMetricsProvider{
		metrics: map[int16]ShardMetrics{
			0: {ShardID: 0, CPUUsage: 95.0, MemoryPct: 90.0, OpsPerSec: 60000}, // Overloaded
			1: {ShardID: 1, CPUUsage: 10.0, MemoryPct: 15.0, OpsPerSec: 1000},  // Healthy
		},
	}

	orchestrator := NewShardOrchestrator(svc, provider, 10*time.Millisecond)
	orchestrator.scaleThreshold = 0.85
	orchestrator.overloadLimit = 20 * time.Millisecond
	orchestrator.cooldown = 0 // disable cooldown for test

	// Initial tick to start overload window
	orchestrator.tick(ctx)
	time.Sleep(30 * time.Millisecond)

	// Second tick to trigger migration after overloadLimit exceeded
	orchestrator.tick(ctx)

	// Verify migration occurred
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM campaign_shard_assignment WHERE campaign_id = $1", ingestion.ToUUID(campID)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify keys were copied to target shard (rdb1)
	exists, err := rdb1.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), exists)

	// Verify keys were drained from source shard (rdb0)
	existsSource, err := rdb0.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), existsSource)

	logChaosProof(t, "campaign_routing_migration", map[string]string{
		"subsystem":         "shard_orchestrator",
		"source_shard":      "0",
		"target_shard":      "1",
		"migration_success": "true",
		"keys_drained":      "true",
	})
}

package management

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockShardMetricsProvider injects synthetic shard load for control-plane chaos tests.
// Production uses RealShardMetricsProvider (Redis INFO); the mock is intentional so
// autoscale triggers are deterministic without saturating real Redis instances.
type mockShardMetricsProvider struct {
	metrics map[int16]ShardMetrics
}

func (p *mockShardMetricsProvider) GetMetrics(ctx context.Context, shardID int16, rdb redis.UniversalClient) (ShardMetrics, error) {
	m, ok := p.metrics[shardID]
	if !ok {
		return ShardMetrics{ShardID: shardID}, nil
	}
	return m, nil
}

// TestChaos_ShardAutoscale_SuddenLoadSpike simulates a sudden CPU and Memory spike on Shard 0.
// It verifies that the autoscaler automatically triggers slot rebalancing, copies campaign data,
// and activates the new slot map version without split-brain or data loss.
func TestChaos_ShardAutoscale_SuddenLoadSpike(t *testing.T) {
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
	svc := NewService(pool, []redis.UniversalClient{rdb0, rdb1}, ads.NewStaticSlotSharder(2), cfg)
	defer svc.Close()

	// Seed active slot map version 1
	mapRepo := ads.NewSlotMapRepo(pool)
	activeVer, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)

	// Pick slot 0 (mapped to Shard 0) and seed a campaign that hashes to slot 0
	var campID uuid.UUID
	var slot int16 = 0
	for {
		campID = uuid.New()
		if ads.CampaignSlotIndex(campID) == slot {
			break
		}
	}

	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Autoscale Cust", 1_000_000, "USD"))

	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'autoscale-test', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ads.ToUUID(campID), ads.ToUUID(customerID))
	require.NoError(t, err)

	key := "budget:campaign:" + campID.String()
	require.NoError(t, rdb0.Set(ctx, key, "850000", 0).Err())

	// Inject sudden load spike on Shard 0
	provider := &mockShardMetricsProvider{
		metrics: map[int16]ShardMetrics{
			0: {ShardID: 0, CPUUsage: 95.0, MemoryPct: 90.0, OpsPerSec: 60000, LuaP99Ms: 25.0}, // Overloaded
			1: {ShardID: 1, CPUUsage: 10.0, MemoryPct: 15.0, OpsPerSec: 1000, LuaP99Ms: 1.0},   // Underloaded
		},
	}

	autoscaleCfg := ShardAutoscaleConfig{
		Enabled:        true,
		CPULimit:       80.0,
		MemoryPctLimit: 85.0,
		OpsLimit:       50000,
		LuaP99Limit:    15.0,
		SlotsToMigrate: 1,
	}

	// Trigger autoscaling
	newVer, err := svc.AutoscaleShards(ctx, provider, autoscaleCfg)
	require.NoError(t, err)
	assert.True(t, newVer > activeVer, "expected a new slot map version to be created and activated")

	// Verify the active version is updated to the new version
	activeVerAfter, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, newVer, activeVerAfter)

	// Verify slot 0 is now mapped to Shard 1
	rows, err := mapRepo.ListVersion(ctx, newVer)
	require.NoError(t, err)
	assert.Equal(t, int16(1), rows[slot].ShardID)

	// Verify campaign budget keys were successfully copied to Shard 1 and deleted from Shard 0
	val, err := rdb1.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "850000", val)

	existsOnSource := rdb0.Exists(ctx, key).Val()
	assert.Equal(t, int64(0), existsOnSource, "expected old keys to be drained from Shard 0")

	logChaosProof(t, "shard_autoscale_sudden_load_spike", map[string]string{
		"new_version":      strconv.FormatInt(int64(newVer), 10),
		"slot_migrated":    strconv.FormatInt(int64(slot), 10),
		"budget_copied":    "true",
		"metrics_injected": "true",
		"source_shard":     "0",
		"target_shard":     "1",
	})
}

// TestChaos_ShardAutoscale_ShuffledShards verifies that shuffling the order of shards
// does not break the autoscaler's ability to identify the overloaded shard and execute migration.
func TestChaos_ShardAutoscale_ShuffledShards(t *testing.T) {
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

	// Shuffled order: s.rdbs[0] is rdb1, s.rdbs[1] is rdb0
	cfg := &config.Config{SlotMigrationEnabled: false}
	svc := NewService(pool, []redis.UniversalClient{rdb1, rdb0}, ads.NewStaticSlotSharder(2), cfg)
	defer svc.Close()

	mapRepo := ads.NewSlotMapRepo(pool)
	activeVer, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)

	// Pick slot 0 (mapped to Shard 0, which is physically rdb1 in our shuffled list)
	var campID uuid.UUID
	var slot int16 = 0
	for {
		campID = uuid.New()
		if ads.CampaignSlotIndex(campID) == slot {
			break
		}
	}

	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Shuffled Cust", 1_000_000, "USD"))

	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'shuffled-test', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ads.ToUUID(campID), ads.ToUUID(customerID))
	require.NoError(t, err)

	key := "budget:campaign:" + campID.String()
	// Since s.rdbs[0] is rdb1, the campaign (slot 0) budget must be seeded in rdb1!
	require.NoError(t, rdb1.Set(ctx, key, "700000", 0).Err())

	// Inject load spike on Shard 0 (physically rdb1)
	provider := &mockShardMetricsProvider{
		metrics: map[int16]ShardMetrics{
			0: {ShardID: 0, CPUUsage: 90.0, MemoryPct: 90.0, OpsPerSec: 60000, LuaP99Ms: 20.0}, // Overloaded (rdb1)
			1: {ShardID: 1, CPUUsage: 10.0, MemoryPct: 10.0, OpsPerSec: 500, LuaP99Ms: 1.0},    // Underloaded (rdb0)
		},
	}

	autoscaleCfg := ShardAutoscaleConfig{
		Enabled:        true,
		CPULimit:       80.0,
		MemoryPctLimit: 85.0,
		OpsLimit:       50000,
		LuaP99Limit:    15.0,
		SlotsToMigrate: 1,
	}

	// Trigger autoscaling
	newVer, err := svc.AutoscaleShards(ctx, provider, autoscaleCfg)
	require.NoError(t, err)
	assert.True(t, newVer > activeVer)

	// Verify slot 0 is now mapped to Shard 1 (physically rdb0)
	rows, err := mapRepo.ListVersion(ctx, newVer)
	require.NoError(t, err)
	assert.Equal(t, int16(1), rows[slot].ShardID)

	// Verify campaign budget keys were successfully copied to Shard 1 (rdb0) and deleted from Shard 0 (rdb1)
	val, err := rdb0.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "700000", val)

	existsOnSource := rdb1.Exists(ctx, key).Val()
	assert.Equal(t, int64(0), existsOnSource, "expected old keys to be drained from Shard 0 (rdb1)")

	logChaosProof(t, "shard_autoscale_shuffled_shards", map[string]string{
		"new_version":      strconv.FormatInt(int64(newVer), 10),
		"slot_migrated":    strconv.FormatInt(int64(slot), 10),
		"budget_copied":    "true",
		"metrics_injected": "true",
		"shuffled_rdbs":    "true",
	})
}

// TestChaos_ShardAutoscale_ConcurrentAutoscaleDeadlock verifies that concurrent attempts
// to autoscale or rebalance do not result in database deadlocks or corrupted slot maps.
func TestChaos_ShardAutoscale_ConcurrentAutoscaleDeadlock(t *testing.T) {
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
	svc := NewService(pool, []redis.UniversalClient{rdb0, rdb1}, ads.NewStaticSlotSharder(2), cfg)
	defer svc.Close()

	mapRepo := ads.NewSlotMapRepo(pool)
	activeVer, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)

	// Seed multiple campaigns across various slots
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Concurrent Cust", 10_000_000, "USD"))

	var slot0CampID uuid.UUID
	const migratedSlot int16 = 0
	for i := int16(0); i < 5; i++ {
		var campID uuid.UUID
		for {
			campID = uuid.New()
			if ads.CampaignSlotIndex(campID) == i {
				break
			}
		}
		if i == migratedSlot {
			slot0CampID = campID
		}
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
			VALUES ($1, 'concurrent-test-%d', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`, i),
			ads.ToUUID(campID), ads.ToUUID(customerID))
		require.NoError(t, err)

		key := "budget:campaign:" + campID.String()
		require.NoError(t, rdb0.Set(ctx, key, "500000", 0).Err())
	}

	// Inject load spike on Shard 0
	provider := &mockShardMetricsProvider{
		metrics: map[int16]ShardMetrics{
			0: {ShardID: 0, CPUUsage: 95.0, MemoryPct: 90.0, OpsPerSec: 60000, LuaP99Ms: 25.0},
			1: {ShardID: 1, CPUUsage: 10.0, MemoryPct: 15.0, OpsPerSec: 1000, LuaP99Ms: 1.0},
		},
	}

	autoscaleCfg := ShardAutoscaleConfig{
		Enabled:        true,
		CPULimit:       80.0,
		MemoryPctLimit: 85.0,
		OpsLimit:       50000,
		LuaP99Limit:    15.0,
		SlotsToMigrate: 2,
	}

	// Launch concurrent autoscaling workers to stress-test locks and metadata contention
	const concurrency = 4
	var wg sync.WaitGroup
	errorsChan := make(chan error, concurrency)
	var maxNewVer int32

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// Add a tiny random jitter to simulate real-world race conditions
			time.Sleep(time.Duration(rand.Intn(15)) * time.Millisecond)
			newVer, err := svc.AutoscaleShards(ctx, provider, autoscaleCfg)
			if err != nil {
				errorsChan <- err
				return
			}
			if newVer > maxNewVer {
				maxNewVer = newVer
			}
		}(i)
	}

	wg.Wait()
	close(errorsChan)

	// Verify that no deadlocks occurred. Some workers might fail with serialization or lock contention
	// errors, which is expected and safe, but the system must not deadlock or corrupt data.
	var failedCount int
	for err := range errorsChan {
		failedCount++
		t.Logf("Expected concurrency conflict: %v", err)
	}

	activeVerAfter, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	assert.Greater(t, activeVerAfter, activeVer, "at least one autoscale worker must publish a new slot map")

	rows, err := mapRepo.ListVersion(ctx, activeVerAfter)
	require.NoError(t, err)
	assert.Equal(t, int16(1), rows[migratedSlot].ShardID, "slot 0 must migrate off overloaded shard 0")

	slot0Key := "budget:campaign:" + slot0CampID.String()
	budgetCopied := rdb1.Exists(ctx, slot0Key).Val() == 1 && rdb0.Exists(ctx, slot0Key).Val() == 0
	assert.True(t, budgetCopied, "migrated slot campaign budget must be copied to target shard")

	t.Logf("Concurrent autoscaling completed: %d/%d workers finished with expected lock conflicts", failedCount, concurrency)

	logChaosProof(t, "shard_autoscale_concurrent_deadlock", map[string]string{
		"new_version":      strconv.FormatInt(int64(activeVerAfter), 10),
		"slot_migrated":    strconv.FormatInt(int64(migratedSlot), 10),
		"budget_copied":    strconv.FormatBool(budgetCopied),
		"metrics_injected": "true",
		"workers_conflict": strconv.Itoa(failedCount) + "/" + strconv.Itoa(concurrency),
		"max_worker_ver":   strconv.FormatInt(int64(maxNewVer), 10),
	})
}

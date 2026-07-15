package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFraudModelSync_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb1, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	var endpoint string
	switch client := rdb1.(type) {
	case *redis.Client:
		endpoint = client.Options().Addr
	default:
		t.Fatalf("unexpected redis client type")
	}

	rdb2 := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}, DB: 1})
	defer rdb2.Close()

	svc := NewService(pool, []redis.UniversalClient{rdb1, rdb2}, nil, nil)
	svc.Close() // Stop background workers immediately to avoid races

	worker := NewOutboxWorker(svc)
	orchestrator := NewFraudModelSyncOrchestrator(svc)
	ctx := context.Background()

	// 1. Seed active model version v1
	_, err := pool.Exec(ctx, `
		INSERT INTO ml_model_versions (id, artifact_hash, status)
		VALUES ('v1', 'hash1', 'ACTIVE')`)
	require.NoError(t, err)

	// Seed syncing model version v2
	_, err = pool.Exec(ctx, `
		INSERT INTO ml_model_versions (id, artifact_hash, status)
		VALUES ('v2', 'hash2', 'SYNCING')`)
	require.NoError(t, err)

	// 2. Tick orchestrator: Shard 0 should enter SYNC phase
	err = orchestrator.Tick(ctx)
	require.NoError(t, err)

	var phase string
	err = pool.QueryRow(ctx, "SELECT phase FROM ml_shard_sync_state WHERE shard_id = 0 AND model_version = 'v2'").Scan(&phase)
	require.NoError(t, err)
	assert.Equal(t, "SYNC", phase)

	// 3. Process outbox events: should write v2 and hash2 to Shard 0
	processed, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Greater(t, processed, 0)

	val, err := rdb1.Get(ctx, "ml:model:version").Result()
	require.NoError(t, err)
	assert.Equal(t, "v2", val)

	val, err = rdb1.Get(ctx, "ml:model:hash").Result()
	require.NoError(t, err)
	assert.Equal(t, "hash2", val)

	// 4. Tick orchestrator again: Shard 0 canary passes, so Shard 0 enters ACTIVE phase.
	err = orchestrator.Tick(ctx)
	require.NoError(t, err)

	err = pool.QueryRow(ctx, "SELECT phase FROM ml_shard_sync_state WHERE shard_id = 0 AND model_version = 'v2'").Scan(&phase)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", phase)

	// Tick orchestrator again: Shard 1 should enter SYNC phase.
	err = orchestrator.Tick(ctx)
	require.NoError(t, err)

	err = pool.QueryRow(ctx, "SELECT phase FROM ml_shard_sync_state WHERE shard_id = 1 AND model_version = 'v2'").Scan(&phase)
	require.NoError(t, err)
	assert.Equal(t, "SYNC", phase)

	// 5. Process outbox events: should write v2 and hash2 to Shard 1
	processed, err = worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Greater(t, processed, 0)

	val, err = rdb2.Get(ctx, "ml:model:version").Result()
	require.NoError(t, err)
	assert.Equal(t, "v2", val)

	// 6. Tick orchestrator again: Shard 1 canary passes, so Shard 1 enters ACTIVE phase.
	err = orchestrator.Tick(ctx)
	require.NoError(t, err)

	err = pool.QueryRow(ctx, "SELECT phase FROM ml_shard_sync_state WHERE shard_id = 1 AND model_version = 'v2'").Scan(&phase)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", phase)

	// 7. Tick orchestrator again: All shards are ACTIVE on v2.
	// The orchestrator should update v2 status to ACTIVE in ml_model_versions, and v1 status to RETIRED.
	err = orchestrator.Tick(ctx)
	require.NoError(t, err)

	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM ml_model_versions WHERE id = 'v2'").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", status)

	err = pool.QueryRow(ctx, "SELECT status FROM ml_model_versions WHERE id = 'v1'").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "RETIRED", status)

	logChaosProof(t, "ml_model_sync_single_shard", map[string]string{
		"subsystem":  "management",
		"shards":     "2",
		"canary_ok":  "true",
		"cutover_ok": "true",
		"status":     "success",
	})
}

func TestFraudModelSync_CanaryRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, nil)
	svc.Close() // Stop background workers immediately to avoid races

	orchestrator := NewFraudModelSyncOrchestrator(svc)
	ctx := context.Background()

	// 1. Seed active model version v1
	_, err := pool.Exec(ctx, `
		INSERT INTO ml_model_versions (id, artifact_hash, status)
		VALUES ('v1', 'hash1', 'ACTIVE')`)
	require.NoError(t, err)

	// Seed syncing model version v2
	_, err = pool.Exec(ctx, `
		INSERT INTO ml_model_versions (id, artifact_hash, status)
		VALUES ('v2', 'hash2', 'SYNCING')`)
	require.NoError(t, err)

	// Set Shard 0 to SYNC phase
	_, err = pool.Exec(ctx, `
		INSERT INTO ml_shard_sync_state (shard_id, model_version, phase, started_at)
		VALUES (0, 'v2', 'SYNC', NOW() - INTERVAL '200 SECONDS')`)
	require.NoError(t, err)

	// 2. Tick orchestrator: Shard 0 has timed out (started_at > 180s ago).
	// It should trigger rollback.
	err = orchestrator.Tick(ctx)
	require.NoError(t, err)

	var phase string
	err = pool.QueryRow(ctx, "SELECT phase FROM ml_shard_sync_state WHERE shard_id = 0 AND model_version = 'v2'").Scan(&phase)
	require.NoError(t, err)
	assert.Equal(t, "ROLLBACK", phase)

	// Verify Shard 0 was rolled back to v1 in Redis
	val, err := rdb.Get(ctx, "ml:model:version").Result()
	require.NoError(t, err)
	assert.Equal(t, "v1", val)

	logChaosProof(t, "ml_model_cutover_rollback", map[string]string{
		"subsystem":   "management",
		"shards":      "1",
		"timeout":     "true",
		"rollback_ok": "true",
		"status":      "success",
	})
}

func TestFraudModelSync_StaleEpochTighten(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	// Seed system settings
	_, err := pool.Exec(context.Background(), `
		INSERT INTO system_settings (key, value)
		VALUES ('fraud_rl_suspect_pct', '50')
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`)
	require.NoError(t, err)

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, nil)
	svc.Close() // Stop background workers immediately to avoid races

	ctx := context.Background()

	// Seed stale applied_at in Redis (now - 1000s)
	rdb.Set(ctx, "ml:model:applied_at", time.Now().Unix()-1000, 0)

	// Call CheckAndHandleStaleEpochs
	err = svc.CheckAndHandleStaleEpochs(ctx)
	require.NoError(t, err)

	// Verify setting was halved in DB (50 / 2 = 25)
	var val string
	err = pool.QueryRow(ctx, "SELECT value FROM system_settings WHERE key = 'fraud_rl_suspect_pct'").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "25", val)

	// Verify that an UPDATE_SETTINGS outbox event was created
	var exists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM outbox_events WHERE event_type = 'UPDATE_SETTINGS')").Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)

	logChaosProof(t, "ml_epoch_gap_tighten", map[string]string{
		"subsystem":   "management",
		"stale":       "true",
		"tightened":   "true",
		"suspect_pct": "25",
		"status":      "success",
	})
}

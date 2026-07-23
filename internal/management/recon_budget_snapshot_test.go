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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReconcileBudgetSnapshot_detectsDriftAndEnqueuesAdjust(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	customerID := uuid.New()
	campaignID := uuid.New()
	const budgetLimit = 10_000_000

	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'snap-recon', 100000000, 'USD')`,
		ingestion.ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window, updated_at)
		VALUES ($1, 'snap-recon', $2, 0, 'ACTIVE', $3, 'ASAP', 'UTC', 86400, NOW() - INTERVAL '1 hour')`,
		ingestion.ToUUID(campaignID), budgetLimit, ingestion.ToUUID(customerID))
	require.NoError(t, err)

	// PG remaining = 10M; Redis total = 9M → drift 1M (within default chunk).
	require.NoError(t, rdb.Set(ctx, ingestion.BudgetCampaignKey(campaignID), 9_000_000, 0).Err())
	require.NoError(t, rdb.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())

	cfg := &config.Config{QuotaChunkSize: 5_000_000, BudgetSyncIntervalMs: 5000, LedgerBatchFlushMs: 10000}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	worker := NewReconWorker(svc, time.Hour)

	worker.ReconcileBudgetSnapshot(ctx)

	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox_events WHERE event_type = 'RECONCILIATION_ADJUST'`).Scan(&outboxCount))
	assert.Equal(t, 1, outboxCount)

	ob := NewOutboxWorker(svc)
	require.NoError(t, ob.ProcessOutbox(ctx))

	var spend int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_spend FROM campaigns WHERE id = $1`,
		ingestion.ToUUID(campaignID)).Scan(&spend))
	assert.Equal(t, int64(1_000_000), spend)
}

func TestReconcileBudgetSnapshot_skipsInflightGrace(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	customerID := uuid.New()
	campaignID := uuid.New()
	tag := "{" + campaignID.String() + "}"
	idStr := campaignID.String()

	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'grace', 100000000, 'USD')`,
		ingestion.ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window, updated_at)
		VALUES ($1, 'grace', 10000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400, NOW())`,
		ingestion.ToUUID(campaignID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	require.NoError(t, rdb.Set(ctx, ingestion.BudgetCampaignKey(campaignID), 5_000_000, 0).Err())
	require.NoError(t, rdb.Set(ctx, tag+"budget:inflight:campaign:"+idStr, 4_000_000, 0).Err())
	require.NoError(t, rdb.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())

	cfg := &config.Config{BudgetSyncIntervalMs: 5000, LedgerBatchFlushMs: 10000}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	worker := NewReconWorker(svc, time.Hour)
	worker.ReconcileBudgetSnapshot(ctx)

	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox_events WHERE event_type = 'RECONCILIATION_ADJUST'`).Scan(&outboxCount))
	assert.Equal(t, 0, outboxCount, "inflight within grace must not enqueue correction")
}

func TestReconcileBudgetSnapshot_skipsMigrationFence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	customerID := uuid.New()
	campaignID := uuid.New()

	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'fence', 100000000, 'USD')`,
		ingestion.ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window, updated_at)
		VALUES ($1, 'fence', 10000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400, NOW() - INTERVAL '1 hour')`,
		ingestion.ToUUID(campaignID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	require.NoError(t, rdb.Set(ctx, ingestion.BudgetCampaignKey(campaignID), 1_000_000, 0).Err())
	require.NoError(t, rdb.Set(ctx, ingestion.MigrationFenceRedisKey(campaignID), "1", 0).Err())
	require.NoError(t, rdb.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())

	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, &config.Config{})
	NewReconWorker(svc, time.Hour).ReconcileBudgetSnapshot(ctx)

	var discCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM recon_discrepancies`).Scan(&discCount))
	assert.Equal(t, 0, discCount)
}

func TestReconToleranceMicro_unit(t *testing.T) {
	t.Parallel()
	assert.Equal(t, int64(1), reconToleranceMicro(0))
	assert.Equal(t, int64(1000), reconToleranceMicro(10_000_000))
}

func TestReconGraceWindow_fromConfig(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{LedgerBatchFlushMs: 10000, BudgetSyncIntervalMs: 5000}
	assert.Equal(t, 15*time.Second, reconGraceWindow(cfg))
}

// TestChaos_ReconUnderLoad runs snapshot recon while Redis has inflight/sync state and verifies budget invariant (M3-15).
func TestChaos_ReconUnderLoad(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	customerID := uuid.New()
	campaignID := uuid.New()
	tag := "{" + campaignID.String() + "}"
	idStr := campaignID.String()
	const budgetLimit = 20_000_000

	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'recon-load', 200000000, 'USD')`,
		ingestion.ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window, updated_at)
		VALUES ($1, 'recon-load', $2, 2000000, 'ACTIVE', $3, 'ASAP', 'UTC', 86400, NOW() - INTERVAL '1 hour')`,
		ingestion.ToUUID(campaignID), budgetLimit, ingestion.ToUUID(customerID))
	require.NoError(t, err)

	require.NoError(t, rdb.Set(ctx, ingestion.BudgetCampaignKey(campaignID), 16_000_000, 0).Err())
	require.NoError(t, rdb.Set(ctx, ingestion.CampaignSyncKey(campaignID), 1_000_000, 0).Err())
	require.NoError(t, rdb.Set(ctx, tag+"budget:inflight:campaign:"+idStr, 500_000, 0).Err())
	require.NoError(t, rdb.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())

	cfg := &config.Config{QuotaChunkSize: 5_000_000, BudgetSyncIntervalMs: 5000, LedgerBatchFlushMs: 10000}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	worker := NewReconWorker(svc, time.Hour)

	worker.ReconcileBudgetSnapshot(ctx)
	ob := NewOutboxWorker(svc)
	require.NoError(t, ob.ProcessOutbox(ctx))

	ingestion.AssertBudgetInvariant(t, ctx, pool, rdb, campaignID)

	logChaosProof(t, "recon_under_load", map[string]string{
		"subsystem":      "management_recon",
		"campaign_id":    campaignID.String(),
		"invariant_ok":   "true",
		"outbox_applied": "true",
	})
}

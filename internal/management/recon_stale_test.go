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

func TestReconcileWindow_skipsAutoAdjustWhenDeltaExceedsChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{QuotaChunkSize: 1_000_000}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	recon := NewReconService(svc)
	ctx := context.Background()

	customerID := uuid.New()
	campaignID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'recon-chunk', 0, 'USD')`,
		ingestion.ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'recon-chunk', 100000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campaignID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	start := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	end := start.Add(time.Hour)

	_, err = pool.Exec(ctx, `
		INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, created_at)
		VALUES ($1, $2, $3, 'FEE', $4)`,
		ingestion.ToUUID(customerID), ingestion.ToUUID(campaignID), -500_000, start.Add(10*time.Minute))
	require.NoError(t, err)

	syncKey := ingestion.CampaignSyncKey(campaignID)
	require.NoError(t, rdb.Set(ctx, syncKey, 10_000_000, 0).Err())

	require.NoError(t, recon.ReconcileWindow(ctx, start, end))

	var adjusted bool
	err = pool.QueryRow(ctx, `
		SELECT redis_adjusted FROM recon_discrepancies WHERE campaign_id = $1 ORDER BY id DESC LIMIT 1`,
		ingestion.ToUUID(campaignID)).Scan(&adjusted)
	require.NoError(t, err)
	assert.False(t, adjusted, "large delta must not be auto-adjusted")

	var ledgerAdjust int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM balance_ledger WHERE campaign_id = $1 AND type = 'RECONCILIATION_ADJUST'`,
		ingestion.ToUUID(campaignID)).Scan(&ledgerAdjust)
	require.NoError(t, err)
	assert.Equal(t, 0, ledgerAdjust)
}

func TestReconcileWindow_autoAdjustsWithinChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{QuotaChunkSize: 5_000_000}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	recon := NewReconService(svc)
	ctx := context.Background()

	customerID := uuid.New()
	campaignID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'recon-ok', 0, 'USD')`,
		ingestion.ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'recon-ok', 100000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campaignID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	start := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	end := start.Add(time.Hour)

	_, err = pool.Exec(ctx, `
		INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, created_at)
		VALUES ($1, $2, $3, 'FEE', $4)`,
		ingestion.ToUUID(customerID), ingestion.ToUUID(campaignID), -500_000, start.Add(10*time.Minute))
	require.NoError(t, err)

	syncKey := ingestion.CampaignSyncKey(campaignID)
	require.NoError(t, rdb.Set(ctx, syncKey, 1_000_000, 0).Err())

	require.NoError(t, recon.ReconcileWindow(ctx, start, end))

	ob := NewOutboxWorker(svc)
	require.NoError(t, ob.ProcessOutbox(ctx))

	var adjusted bool
	err = pool.QueryRow(ctx, `
		SELECT redis_adjusted FROM recon_discrepancies WHERE campaign_id = $1 ORDER BY id DESC LIMIT 1`,
		ingestion.ToUUID(campaignID)).Scan(&adjusted)
	require.NoError(t, err)
	assert.True(t, adjusted)
}

func TestAlertStaleUnresolvedDiscrepancies_notifiesOps(t *testing.T) {
	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true
	svc := &Service{alerter: NewOpsAlerter(&NotifierClient{client: stub}, cfg)}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	svc.SetPool(pool)
	recon := NewReconService(svc)
	ctx := context.Background()

	var runID int64
	start := time.Date(2026, 7, 4, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO recon_runs (period_start, period_end, status, completed_at)
		VALUES ($1, $2, 'COMPLETED', NOW()) RETURNING id`, start, end).Scan(&runID))

	campaignID := uuid.New()
	customerID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO recon_discrepancies (run_id, campaign_id, customer_id, expected_spend, actual_spend, delta, redis_adjusted, created_at)
		VALUES ($1, $2, $3, 5000000, 1000000, 4000000, false, NOW() - INTERVAL '90 minutes')`,
		runID, campaignID, customerID)
	require.NoError(t, err)

	recon.AlertStaleUnresolvedDiscrepancies(ctx)
	time.Sleep(150 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)
	assert.Contains(t, requests[0].Body, "Unresolved recon discrepancy")
	assert.Equal(t, "recon:unresolved:"+itoaMgmtChaos(int(runID)), requests[0].DedupKey)
}

// TestChaos_ReconStaleDiscrepancyOpsAlert guards unresolved drift older than 1h reaches notifier.
func TestChaos_ReconStaleDiscrepancyOpsAlert(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	stub := &stubNotifierGRPCClient{}
	cfg := testNotifierConfig()
	cfg.Management.OpsAlertsEnabled = true

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	svc := newBareService(t, pool, nil, cfg)
	svc.alerter = NewOpsAlerter(&NotifierClient{client: stub}, cfg)
	recon := NewReconService(svc)
	ctx := context.Background()

	var runID int64
	periodStart := time.Date(2026, 7, 3, 8, 0, 0, 0, time.UTC)
	periodEnd := periodStart.Add(time.Hour)
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO recon_runs (period_start, period_end, status, completed_at)
		VALUES ($1, $2, 'COMPLETED', NOW()) RETURNING id`, periodStart, periodEnd).Scan(&runID))

	_, err := pool.Exec(ctx, `
		INSERT INTO recon_discrepancies (run_id, campaign_id, customer_id, expected_spend, actual_spend, delta, redis_adjusted, created_at)
		VALUES ($1, $2, $3, 9000000, 1000000, 8000000, false, NOW() - INTERVAL '2 hours')`,
		runID, uuid.New(), uuid.New())
	require.NoError(t, err)

	recon.AlertStaleUnresolvedDiscrepancies(ctx)
	time.Sleep(200 * time.Millisecond)

	requests := stub.snapshot()
	require.Len(t, requests, 1)

	logChaosProof(t, "recon_stale_discrepancy_ops_alert", map[string]string{
		"subsystem":   "management_recon",
		"run_id":      itoaMgmtChaos(int(runID)),
		"notified":    "true",
		"baseline_ok": "true",
		"fault_type":  "stale_unresolved",
	})
}

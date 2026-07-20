package ingestion

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLedgerBatch_ConsolidatesDeltas verifies 100 Redis sync increments produce one ledger FEE row.
func TestLedgerBatch_ConsolidatesDeltas(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	pm := database.NewPartitionManager(infra.Pool, 7, 1)
	require.NoError(t, pm.Run(ctx))

	customerID := uuid.New()
	_, err := infra.Pool.Exec(ctx,
		"INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)",
		customerID, "Batch Customer", 500_000_000)
	require.NoError(t, err)

	campaignID := uuid.New()
	const budgetLimit = 100_000_000
	_, err = infra.Pool.Exec(ctx,
		"INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
		campaignID, "Batch Campaign", "ACTIVE", customerID, budgetLimit)
	require.NoError(t, err)

	const deltaMicro = int64(1_000)
	const deltaCount = 100
	var totalMicro int64
	syncKey := "budget:sync:campaign:" + campaignID.String()
	budgetKey := "budget:campaign:" + campaignID.String()

	for i := 0; i < deltaCount; i++ {
		require.NoError(t, infra.Redis.IncrBy(ctx, syncKey, deltaMicro).Err())
		totalMicro += deltaMicro
	}
	require.NoError(t, infra.Redis.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())
	require.NoError(t, infra.Redis.Set(ctx, budgetKey, budgetLimit-totalMicro, 0).Err())

	campaignRepo := NewCampaignRepoWithDB(infra.Pool, infra.Queries)
	worker := NewSyncWorker(infra.Redis, campaignRepo, nil, time.Hour, 0, nil, 0)
	worker.SyncAll(ctx)

	var ledgerCount int
	err = infra.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM balance_ledger WHERE campaign_id = $1 AND type = 'FEE'`, ToUUID(campaignID),
	).Scan(&ledgerCount)
	require.NoError(t, err)
	assert.Equal(t, 1, ledgerCount, "consolidated flush must emit exactly one ledger row")

	var ledgerAmount int64
	err = infra.Pool.QueryRow(ctx,
		`SELECT amount FROM balance_ledger WHERE campaign_id = $1 AND type = 'FEE'`, ToUUID(campaignID),
	).Scan(&ledgerAmount)
	require.NoError(t, err)
	assert.Equal(t, -totalMicro, ledgerAmount)

	var currentSpend int64
	err = infra.Pool.QueryRow(ctx,
		`SELECT current_spend FROM campaigns WHERE id = $1`, ToUUID(campaignID),
	).Scan(&currentSpend)
	require.NoError(t, err)
	assert.Equal(t, totalMicro, currentSpend)

	AssertBudgetInvariant(t, ctx, infra.Pool, infra.Redis, campaignID)
}

// TestLedgerBatch_ZeroBalancePausesCampaign pauses delivery when customer funds cannot cover the batch.
func TestLedgerBatch_ZeroBalancePausesCampaign(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	_, err := infra.Pool.Exec(ctx,
		"INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)",
		customerID, "Low Balance", 5_000)
	require.NoError(t, err)

	campaignID := uuid.New()
	_, err = infra.Pool.Exec(ctx,
		"INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
		campaignID, "Spend Campaign", "ACTIVE", customerID, 100_000_000)
	require.NoError(t, err)

	syncKey := "budget:sync:campaign:" + campaignID.String()
	require.NoError(t, infra.Redis.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())
	require.NoError(t, infra.Redis.Set(ctx, syncKey, 50_000, 0).Err())

	campaignRepo := NewCampaignRepoWithDB(infra.Pool, infra.Queries)
	worker := NewSyncWorker(infra.Redis, campaignRepo, nil, time.Hour, 0, nil, 0)
	worker.SyncAll(ctx)

	var status string
	err = infra.Pool.QueryRow(ctx, `SELECT status FROM campaigns WHERE id = $1`, ToUUID(campaignID)).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "PAUSED", status)
}

// TestLedgerBatch_MultiCampaignSingleTxn verifies multiple campaigns flush in one batch txn.
func TestLedgerBatch_MultiCampaignSingleTxn(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	_, err := infra.Pool.Exec(ctx,
		"INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)",
		customerID, "Multi Batch", 1_000_000_000)
	require.NoError(t, err)

	const campaignCount = 10
	const deltaMicro = int64(2_500)
	campaignIDs := make([]uuid.UUID, campaignCount)

	for i := 0; i < campaignCount; i++ {
		campaignIDs[i] = uuid.New()
		_, err = infra.Pool.Exec(ctx,
			"INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
			campaignIDs[i], "Batch Camp", "ACTIVE", customerID, 100_000_000)
		require.NoError(t, err)

		syncKey := "budget:sync:campaign:" + campaignIDs[i].String()
		require.NoError(t, infra.Redis.SAdd(ctx, "budget:dirty_campaigns", campaignIDs[i].String()).Err())
		require.NoError(t, infra.Redis.Set(ctx, syncKey, deltaMicro, 0).Err())
	}

	campaignRepo := NewCampaignRepoWithDB(infra.Pool, infra.Queries)
	worker := NewSyncWorker(infra.Redis, campaignRepo, nil, time.Hour, 0, nil, 0)
	worker.SyncAll(ctx)

	var ledgerCount int
	err = infra.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM balance_ledger WHERE customer_id = $1 AND type = 'FEE'`, ToUUID(customerID),
	).Scan(&ledgerCount)
	require.NoError(t, err)
	assert.Equal(t, campaignCount, ledgerCount)

	var auditCount int
	err = infra.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM admin_audit_log WHERE action = 'LEDGER_BATCH_FLUSH'`,
	).Scan(&auditCount)
	require.NoError(t, err)
	assert.Equal(t, 0, auditCount, "audit disabled by default")
}

// TestLedgerBatch_AuditSampling writes LEDGER_BATCH_FLUSH rows only when sampling is enabled.
func TestLedgerBatch_AuditSampling(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	_, err := infra.Pool.Exec(ctx,
		"INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)",
		customerID, "Audit Sample", 500_000_000)
	require.NoError(t, err)

	campaignRepo := NewCampaignRepoWithDB(infra.Pool, infra.Queries)
	campaignRepo.ConfigureAuditLedgerFlush(1) // mask=1 → ~50% sample for deterministic integration proof

	const flushCount = 64
	for i := 0; i < flushCount; i++ {
		campaignID := uuid.New()
		_, err = infra.Pool.Exec(ctx,
			"INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)",
			campaignID, "Audit Camp", "ACTIVE", customerID, 100_000_000)
		require.NoError(t, err)

		syncKey := "budget:sync:campaign:" + campaignID.String()
		require.NoError(t, infra.Redis.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())
		require.NoError(t, infra.Redis.Set(ctx, syncKey, 1_000, 0).Err())
	}

	worker := NewSyncWorker(infra.Redis, campaignRepo, nil, time.Hour, 0, nil, 0)
	worker.SyncAll(ctx)

	var ledgerCount int
	err = infra.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM balance_ledger WHERE customer_id = $1 AND type = 'FEE'`, ToUUID(customerID),
	).Scan(&ledgerCount)
	require.NoError(t, err)
	assert.Equal(t, flushCount, ledgerCount)

	var auditCount int
	err = infra.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM admin_audit_log WHERE action = 'LEDGER_BATCH_FLUSH'`,
	).Scan(&auditCount)
	require.NoError(t, err)
	assert.Greater(t, auditCount, 0)
	assert.Less(t, auditCount, flushCount, "sampling must drop most flush audit rows")
}

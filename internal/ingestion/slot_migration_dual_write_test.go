package ingestion

import (
	"context"
	"testing"

	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSlotMigrationDualWrite_CatchUpAndLag(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	src, cleanupSrc := database.SetupTestRedis(t)
	defer cleanupSrc()
	dst, cleanupDst := database.SetupTestRedis(t)
	defer cleanupDst()

	const version int32 = 2
	const slot int16 = 4
	campID := uuid.New()
	spendKey := BudgetCampaignKey(campID)
	require.NoError(t, src.Set(ctx, spendKey, 500000, 0).Err())
	require.NoError(t, dst.Set(ctx, spendKey, 500000, 0).Err())

	require.NoError(t, EnableSlotMigrationDualWrite(ctx, src, version, slot, 1))
	require.NoError(t, PublishSlotMigrationDeltaTestHelper(ctx, src, SlotMigrationDelta{
		CampaignID: campID,
		Amount:     1000,
		SpendKey:   spendKey,
	}))

	lag, err := SlotMigrationReplicationLag(ctx, src)
	require.NoError(t, err)
	require.Equal(t, int64(1), lag)

	applied, lagAfter, err := CatchUpSlotMigrationDeltas(ctx, src, dst, version, slot)
	require.NoError(t, err)
	require.Equal(t, 1, applied)
	require.Equal(t, int64(0), lagAfter)

	remaining, err := dst.Get(ctx, spendKey).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(499000), remaining)

	syncDelta, err := dst.Get(ctx, CampaignSyncKey(campID)).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1000), syncDelta)
}

func TestVerifyBudgetInvariant_afterDualWriteCatchUp(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	src, cleanupSrc := database.SetupTestRedis(t)
	defer cleanupSrc()
	dst, cleanupDst := database.SetupTestRedis(t)
	defer cleanupDst()

	campID := uuid.New()
	customerID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'dw', 0, 'USD')`,
		ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'dw', 1000000, 1000, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ToUUID(campID), ToUUID(customerID))
	require.NoError(t, err)

	spendKey := BudgetCampaignKey(campID)
	require.NoError(t, src.Set(ctx, spendKey, 998000, 0).Err())
	require.NoError(t, dst.Set(ctx, spendKey, 999000, 0).Err())
	require.NoError(t, PublishSlotMigrationDeltaTestHelper(ctx, src, SlotMigrationDelta{
		CampaignID: campID,
		Amount:     1000,
		SpendKey:   spendKey,
	}))
	_, _, err = CatchUpSlotMigrationDeltas(ctx, src, dst, 1, 3)
	require.NoError(t, err)

	require.NoError(t, VerifyBudgetInvariant(ctx, pool, dst, campID))
}

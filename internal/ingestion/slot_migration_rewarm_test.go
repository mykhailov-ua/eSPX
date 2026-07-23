package ingestion

import (
	"context"
	"testing"

	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRewarmCampaignBudgetKeys_FromPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	campID := uuid.New()
	customerID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'rewarm', 0, 'USD')`,
		ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'rewarm-camp', 1000000, 250000, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ToUUID(campID), ToUUID(customerID))
	require.NoError(t, err)

	require.NoError(t, RewarmCampaignBudgetKeys(ctx, pool, rdb, []uuid.UUID{campID}))

	key := budgetCampaignKey(campID)
	val, err := rdb.Get(ctx, key).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(750000), val)
}

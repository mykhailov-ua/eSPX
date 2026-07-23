package ingestion

import (
	"context"
	"testing"

	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchBudgetReconSnapshot_atomic(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	tag := campaignHashTag(campID)
	idStr := campID.String()

	require.NoError(t, rdb.Set(ctx, budgetCampaignKey(campID), 8_000_000, 0).Err())
	require.NoError(t, rdb.Set(ctx, campaignSyncKey(campID), 500_000, 0).Err())
	require.NoError(t, rdb.Set(ctx, tag+"budget:inflight:campaign:"+idStr, 200_000, 0).Err())

	snap, err := FetchBudgetReconSnapshot(ctx, rdb, campID, false)
	require.NoError(t, err)
	assert.Equal(t, int64(8_000_000), snap.Remaining)
	assert.Equal(t, int64(500_000), snap.Sync)
	assert.Equal(t, int64(200_000), snap.Inflight)
	assert.Equal(t, int64(8_700_000), snap.RedisBudgetRemainingTotal(0))
}

func TestReconToleranceMicro(t *testing.T) {
	t.Parallel()
	// tested via management package helpers; snapshot total is pure sum
	snap := BudgetReconSnapshot{Remaining: 1, Sync: 2, Inflight: 3}
	assert.Equal(t, int64(6), snap.RedisBudgetRemainingTotal(0))
	assert.Equal(t, int64(7), snap.RedisBudgetRemainingTotal(1))
}

func BenchmarkReconcileSnapshot(b *testing.B) {
	ctx := context.Background()
	rdb, cleanup := database.SetupTestRedis(b)
	defer cleanup()
	campID := uuid.New()
	require.NoError(b, rdb.Set(ctx, budgetCampaignKey(campID), 5_000_000, 0).Err())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = FetchBudgetReconSnapshot(ctx, rdb, campID, false)
	}
}

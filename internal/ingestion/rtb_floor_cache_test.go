package ingestion

import (
	"context"
	"testing"

	"espx/internal/database"
	"espx/internal/rtb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEffectiveDealFloor_usesOptimizedWhenHigher(t *testing.T) {
	catalog := NewRtbCatalog(rtb.NewBudgetStore(), BudgetAuthorityShadow)
	catalog.UpdateDeals([]rtb.DealData{{DealID: "deal-a", FloorMicro: 100_000}})
	cache := NewDealFloorCache(nil)
	next := map[string]int64{"deal-a": 150_000}
	cache.snap.Store(&next)

	floor := EffectiveDealFloor(catalog, cache, "deal-a", 80_000)
	assert.Equal(t, int64(150_000), floor)
}

func TestDealFloorCache_RefreshFromRedis(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	require.NoError(t, rdb.Set(context.Background(), RtbFloorRedisKeyPrefix+"deal-x", "250000", 0).Err())
	cache := NewDealFloorCache(rdb)
	cache.Refresh(context.Background(), []string{"deal-x"})
	v, ok := cache.Get("deal-x")
	require.True(t, ok)
	assert.Equal(t, int64(250_000), v)
}

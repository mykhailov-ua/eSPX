package rtbbridge

import (
	"context"
	"testing"

	"espx/internal/ads/catalog"
	"espx/internal/ads/sharding"
	adstest "espx/internal/ads/testutil"
	"espx/internal/domain"
	"espx/internal/metrics"
	"espx/internal/rtb"

	"github.com/google/uuid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReconcileCampaignBudget_detectsDivergence(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &adstest.MockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, int64(1_000_000), 0).Err())

	store := rtb.NewBudgetStore()
	store.SetBudget(CampaignIDFromUUID(campID), 900_000)

	redisRem, rtbRem, ok := ReconcileCampaignBudget(ctx, store, []redis.UniversalClient{rdb}, sharding.NewJumpHashSharder(1), camp)
	require.True(t, ok)
	assert.Equal(t, int64(1_000_000), redisRem)
	assert.Equal(t, int64(900_000), rtbRem)
}

func TestRtbBudgetReconcileWorker_sample(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &adstest.MockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, int64(2_000_000), 0).Err())

	campCopy := *camp
	campCopy.ID = campID
	registry := catalog.NewRegistry(nil)
	registry.Add(campID, campCopy.CustomerID, nil, "", domain.PacingModeAsap, campCopy.BudgetLimit, "UTC", 0, 0, nil)

	store := rtb.NewBudgetStore()
	catalogRTB := NewRtbCatalog(store, BudgetAuthorityRTB)
	store.SetBudget(CampaignIDFromUUID(campID), 1_000_000)

	before := promtest.ToFloat64(metrics.RtbBudgetReconcileSamplesTotal)
	worker := NewRtbBudgetReconcileWorker(
		RtbBudgetReconcileConfig{DivergenceThreshold: 100, SampleSize: 4},
		registry,
		catalogRTB,
		[]redis.UniversalClient{rdb},
		sharding.NewJumpHashSharder(1),
	)
	worker.sample(ctx)
	assert.Greater(t, promtest.ToFloat64(metrics.RtbBudgetReconcileSamplesTotal), before)
}

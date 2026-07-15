package ingestion

import (
	"context"
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/ingestion/sqlc"
	"espx/internal/metrics"
	"espx/internal/rtb"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReconcileCampaignBudget_detectsDivergence(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, int64(1_000_000), 0).Err())

	store := rtb.NewBudgetStore()
	store.SetBudget(CampaignIDFromUUID(campID), 900_000)

	redisRem, rtbRem, ok := ReconcileCampaignBudget(ctx, store, []redis.UniversalClient{rdb}, NewJumpHashSharder(1), camp)
	require.True(t, ok)
	assert.Equal(t, int64(1_000_000), redisRem)
	assert.Equal(t, int64(900_000), rtbRem)
}

func TestRtbBudgetReconcileWorker_sample(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, int64(2_000_000), 0).Err())

	campCopy := *camp
	campCopy.ID = campID
	registry := &Registry{}
	registry.data.Store(&campaignMapSnapshot{byID: map[uuid.UUID]campaignInfo{
		campID: {campaign: &campCopy, status: db.CampaignStatusTypeACTIVE},
	}})

	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)
	store.SetBudget(CampaignIDFromUUID(campID), 1_000_000)

	before := testutil.ToFloat64(metrics.RtbBudgetReconcileSamplesTotal)
	worker := NewRtbBudgetReconcileWorker(
		RtbBudgetReconcileConfig{DivergenceThreshold: 100, SampleSize: 4},
		registry,
		catalog,
		[]redis.UniversalClient{rdb},
		NewJumpHashSharder(1),
	)
	worker.sample(ctx)
	assert.Greater(t, testutil.ToFloat64(metrics.RtbBudgetReconcileSamplesTotal), before)
}

func TestApplyRtbAuction_pacingNoBidRejectKind(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)

	id := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: id, BudgetLimit: 5000}},
		map[uuid.UUID]RtbCampaignInput{
			id: {
				BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: geo, Weight: 1,
				PacingOpen: rtb.PacingClosed,
			},
		},
	)

	proc := trackProcessor{rtbCatalog: catalog, rtbMode: rtbModeLive, ingestGeo: &staticGeoProvider{country: "US"}}
	evt := &campaignmodel.Event{CampaignID: uuid.New(), IP: "8.8.8.8"}
	ensureIngestGeo(proc.ingestGeo, evt)

	out, handled := applyRtbAuction(proc, evt, nil)
	require.True(t, handled)
	assert.Equal(t, filterRejectPacing, out.RejectKind)
}

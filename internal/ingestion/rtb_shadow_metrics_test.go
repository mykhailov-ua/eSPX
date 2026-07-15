package ingestion

import (
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/metrics"
	"espx/internal/rtb"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordRtbShadowAuction_winnerMismatch(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)

	winnerID := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: winnerID, BudgetLimit: 5000, TargetCountries: map[string]struct{}{"US": {}}}},
		map[uuid.UUID]RtbCampaignInput{
			winnerID: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: geo, Weight: 1},
		},
	)

	clientID := uuid.New()
	evt := &campaignmodel.Event{CampaignID: clientID, IP: "8.8.8.8"}
	proc := trackProcessor{
		rtbCatalog: catalog,
		rtbMode:    rtbModeShadow,
		ingestGeo:  &staticGeoProvider{country: "US"},
	}
	ensureIngestGeo(proc.ingestGeo, evt)

	before := testutil.ToFloat64(metrics.RtbShadowWinnerMismatchTotal)
	_, handled := applyRtbAuction(proc, evt, nil)
	require.False(t, handled)
	assert.Equal(t, clientID, evt.CampaignID)
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.RtbShadowWinnerMismatchTotal))
}

func TestRecordRtbShadowAuction_winnerMatch(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)

	winnerID := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: winnerID, BudgetLimit: 5000, TargetCountries: map[string]struct{}{"US": {}}}},
		map[uuid.UUID]RtbCampaignInput{
			winnerID: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: geo, Weight: 1},
		},
	)

	evt := &campaignmodel.Event{CampaignID: winnerID, IP: "8.8.8.8"}
	proc := trackProcessor{
		rtbCatalog: catalog,
		rtbMode:    rtbModeShadow,
		ingestGeo:  &staticGeoProvider{country: "US"},
	}
	ensureIngestGeo(proc.ingestGeo, evt)

	before := testutil.ToFloat64(metrics.RtbShadowWinnerMismatchTotal)
	_, handled := applyRtbAuction(proc, evt, nil)
	require.False(t, handled)
	assert.Equal(t, before, testutil.ToFloat64(metrics.RtbShadowWinnerMismatchTotal))
}

func TestRecordRtbShadowAuction_noBid(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)
	catalog.SyncActiveCampaigns(nil, nil)

	evt := &campaignmodel.Event{CampaignID: uuid.New()}
	proc := trackProcessor{rtbCatalog: catalog, rtbMode: rtbModeShadow}

	before := testutil.ToFloat64(metrics.RtbShadowNoBidTotal.WithLabelValues(rtb.NoBidEmptyShard.String()))
	_, handled := applyRtbAuction(proc, evt, nil)
	require.False(t, handled)
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.RtbShadowNoBidTotal.WithLabelValues(rtb.NoBidEmptyShard.String())))
}

func TestApplyRtbAuction_shadow_zeroAlloc(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)

	id := uuid.New()
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: id, BudgetLimit: 5000}},
		map[uuid.UUID]RtbCampaignInput{
			id: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: 0, Weight: 1},
		},
	)

	proc := trackProcessor{
		rtbCatalog: catalog,
		rtbMode:    rtbModeShadow,
	}
	evt := &campaignmodel.Event{CampaignID: id}

	for i := 0; i < 16; i++ {
		_, _ = applyRtbAuction(proc, evt, nil)
	}
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = applyRtbAuction(proc, evt, nil)
	})
	assert.Equal(t, float64(0), allocs)
}

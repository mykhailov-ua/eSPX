package ingestion

import (
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/rtb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessTrack_rtbLiveSelectsWinner(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)

	winnerID := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: winnerID, BudgetLimit: 5000, TargetCountries: map[string]struct{}{"US": {}}}},
		map[uuid.UUID]RtbCampaignInput{
			winnerID: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: geo, Weight: 1},
		},
	)

	proc := trackProcessor{
		rtbCatalog: catalog,
		rtbMode:    rtbModeLive,
		ingestGeo:  &staticGeoProvider{country: "US"},
	}
	evt := &campaignmodel.Event{
		CampaignID: uuid.New(),
		IP:         "8.8.8.8",
		Type:       "click",
	}

	out := processTrack(proc, evt, []byte("desktop"))
	assert.Equal(t, trackStatusAccepted, out.Status)
	assert.Equal(t, winnerID, evt.CampaignID)
}

func TestProcessTrack_rtbLiveNoBidRejects(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)
	catalog.SyncActiveCampaigns(nil, nil)

	proc := trackProcessor{
		rtbCatalog: catalog,
		rtbMode:    rtbModeLive,
	}
	evt := &campaignmodel.Event{CampaignID: uuid.New(), Type: "click"}

	out := processTrack(proc, evt, nil)
	require.Equal(t, trackStatusRejected, out.Status)
	assert.Equal(t, filterRejectBidFloor, out.RejectKind)
}

func TestProcessTrack_rtbShadowKeepsClientCampaign(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)

	id := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: id, BudgetLimit: 5000}},
		map[uuid.UUID]RtbCampaignInput{
			id: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: geo, Weight: 1},
		},
	)

	clientID := uuid.New()
	proc := trackProcessor{
		rtbCatalog: catalog,
		rtbMode:    rtbModeShadow,
		ingestGeo:  &staticGeoProvider{country: "US"},
	}
	evt := &campaignmodel.Event{CampaignID: clientID, IP: "8.8.8.8", Type: "click"}

	out := processTrack(proc, evt, nil)
	assert.Equal(t, trackStatusAccepted, out.Status)
	assert.Equal(t, clientID, evt.CampaignID)
}

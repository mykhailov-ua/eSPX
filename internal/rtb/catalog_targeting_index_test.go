package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTargetingIndex_groupsByGeoDeviceCategory(t *testing.T) {
	reg := &CampaignAuctionRegistry{
		Count:                 3,
		CampaignIDs:           []CampaignID{1, 2, 3},
		Bids:                  []int64{100, 200, 300},
		CTRPPM:                []uint32{CTRPPMUnit, CTRPPMUnit, CTRPPMUnit},
		Reserves:              []int64{0, 0, 0},
		DailyBudgets:          []int64{0, 0, 0},
		PacingOpen:            []uint8{PacingOpen, PacingOpen, PacingOpen},
		DeviceMasks:           []uint8{1, 2, 1},
		CategoryMasks:         []uint64{1, 1, 2},
		GeoHashes:             []uint32{7, 7, 7},
		Weights:               []uint32{1, 1, 1},
		BoostPPM:              []uint32{CTRPPMUnit, CTRPPMUnit, CTRPPMUnit},
		BudgetIndices:         []uint32{0, 1, 2},
		CustomerBudgetIndices: []uint32{invalidCustomerBudgetIdx, invalidCustomerBudgetIdx, invalidCustomerBudgetIdx},
	}
	buildTargetingIndex(reg)

	start, end, ok := reg.targetingRange(7, 1, 1)
	require.True(t, ok)
	assert.Equal(t, 1, end-start)

	_, _, ok = reg.targetingRange(7, 2, 1)
	require.True(t, ok)

	_, _, ok = reg.targetingRange(7, 1, 2)
	require.True(t, ok)

	_, _, ok = reg.targetingRange(7, 4, 1)
	assert.False(t, ok)
}

func TestAuction_targetingIndex_skipsDeviceMismatch(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.SetTargetingIndexEnabled(true)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 500, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: 2, Bid: 100, DeviceMask: 2, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	res, reason := reg.RunAuction(&BidRequest{
		GeoHash: 7, DeviceType: 2, CategoryMask: 1, MinBid: 50,
	})
	require.True(t, reason.OK())
	assert.Equal(t, CampaignID(2), res.CampaignID)
}

func TestAuction_targetingIndex_disabledFallsBackToGeo(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 500, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: 2, Bid: 100, DeviceMask: 2, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	res, reason := reg.RunAuction(&BidRequest{
		GeoHash: 7, DeviceType: 2, CategoryMask: 1, MinBid: 50,
	})
	require.True(t, reason.OK())
	assert.Equal(t, CampaignID(2), res.CampaignID)
}

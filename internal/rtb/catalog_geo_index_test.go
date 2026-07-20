package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards buildGeoIndex groups campaigns by geo hash for bucket iteration.
func TestBuildGeoIndex_groupsByGeo(t *testing.T) {
	reg := &CampaignAuctionRegistry{
		Count:                 4,
		CampaignIDs:           []CampaignID{1, 2, 3, 4},
		Bids:                  []int64{10, 20, 30, 40},
		CTRPPM:                []uint32{CTRPPMUnit, CTRPPMUnit, CTRPPMUnit, CTRPPMUnit},
		Reserves:              []int64{0, 0, 0, 0},
		DailyBudgets:          []int64{0, 0, 0, 0},
		PacingOpen:            []uint8{PacingOpen, PacingOpen, PacingOpen, PacingOpen},
		DeviceMasks:           []uint8{1, 1, 1, 1},
		CategoryMasks:         []uint64{1, 1, 1, 1},
		GeoHashes:             []uint32{7, 7, 9, 9},
		Weights:               []uint32{1, 2, 3, 4},
		BudgetIndices:         []uint32{0, 1, 2, 3},
		CustomerBudgetIndices: []uint32{invalidCustomerBudgetIdx, invalidCustomerBudgetIdx, invalidCustomerBudgetIdx, invalidCustomerBudgetIdx},
	}
	buildGeoIndex(reg)

	start, end, ok := reg.geoRange(7)
	require.True(t, ok)
	assert.Equal(t, 2, end-start)

	_, _, ok = reg.geoRange(8)
	assert.False(t, ok)
}

// Guards geo index limits candidates to the request geo within a shard.
func TestAuction_geoIndex_skipsOtherGeoInShard(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 500, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: 2, Bid: 100, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 71, Budget: 5000},
	})

	res, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, CampaignID(1), res.CampaignID)
}

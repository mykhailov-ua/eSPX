package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards eCTR ranking prefers higher bid*CTR over raw bid.
func TestAuction_eCTR_ranking(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 200, CTRPPM: 500_000, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: 2, Bid: 150, CTRPPM: CTRPPMUnit, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	res, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, CampaignID(2), res.CampaignID)
}

// Guards reserve lifts clearing price above publisher floor.
func TestAuction_reserve_floor(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 200, Reserve: 120, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	res, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, int64(120), res.Price)
}

// Guards first-price clearing charges the winner bid.
func TestAuction_firstPrice(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.SetClearingMode(ClearingFirstPrice)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: 2, Bid: 150, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	res, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, CampaignID(1), res.CampaignID)
	assert.Equal(t, int64(200), res.Price)
}

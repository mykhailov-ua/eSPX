package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDealBlock_shortCircuitsBeforeScan(t *testing.T) {
	SetMetricsEnabled(false)
	store := NewBudgetStore()
	reg := NewRegistry(store)
	geo := uint32(9)
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 500, DeviceMask: 1, CategoryMask: 1, GeoHashVal: geo, Weight: 1, Budget: 1_000_000,
	}})
	req := &BidRequest{
		MinBid: 1, DeviceType: 1, CategoryMask: 1, GeoHash: geo, DealBlock: NoBidDealMismatch,
	}
	_, reason := reg.RunAuction(req)
	require.Equal(t, NoBidDealMismatch, reason)
}

func TestRunAuction_dealMismatchNoBid(t *testing.T) {
	SetMetricsEnabled(false)
	store := NewBudgetStore()
	reg := NewRegistry(store)
	geo := uint32(5)
	reg.UpdateCampaigns([]CampaignData{{
		ID:           1,
		Bid:          500,
		DeviceMask:   1,
		CategoryMask: 4,
		GeoHashVal:   geo,
		Weight:       1,
		Budget:       1_000_000,
	}})

	req := &BidRequest{
		MinBid:       100,
		DeviceType:   1,
		CategoryMask: 4,
		GeoHash:      geo,
		DealBlock:    NoBidDealMismatch,
	}
	_, reason := reg.RunAuction(req)
	require.Equal(t, NoBidDealMismatch, reason)
}

func TestRankCandidates_tmaxTimeout(t *testing.T) {
	SetMetricsEnabled(false)
	store := NewBudgetStore()
	reg := NewRegistry(store)
	geo := uint32(3)
	n := 200
	campaigns := make([]CampaignData, n)
	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           CampaignID(uint64(i + 1)),
			Bid:          10,
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   geo,
			Weight:       uint32(i),
			Budget:       1_000_000_000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	req := &BidRequest{
		MinBid:       50_000,
		DeviceType:   1,
		CategoryMask: 1,
		GeoHash:      geo,
		DeadlineMono: 1,
	}
	_, reason := reg.RunAuction(req)
	assert.Equal(t, NoBidTimeout, reason)
}

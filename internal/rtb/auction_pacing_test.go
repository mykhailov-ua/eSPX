package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards pacing-closed campaigns return NoBidPacingClosed.
func TestAuction_pacingClosed(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{{
		ID: CampaignID(1), Bid: 100, PacingOpen: PacingClosed,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000,
	}})

	_, reason := reg.RunAuction(stdReq(7, 50))
	assert.Equal(t, NoBidPacingClosed, reason)
}

// Guards daily cap blocks candidates before spend when headroom is below bid.
func TestAuction_dailyCap_blocksBeforeSpend(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{{
		ID: CampaignID(1), Bid: 100, DailyBudget: 80,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000,
	}})

	idx := reg.LoadShard(7).BudgetIndices[0]
	store.addDailySpendLocked(idx, 50)

	_, reason := reg.RunAuction(stdReq(7, 50))
	assert.Equal(t, NoBidDailyCapExceeded, reason)
}

// Guards successful spend increments the in-memory daily counter.
func TestAuction_dailyCap_spendTracks(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{{
		ID: CampaignID(1), Bid: 100, DailyBudget: 200,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000,
	}})

	_, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())

	idx := reg.LoadShard(7).BudgetIndices[0]
	assert.Equal(t, int64(50), store.loadOn(&store.dailySpent, idx))
}

// Guards customer-level budget is shared across campaigns of one advertiser.
func TestAuction_customerBudget_sharedPool(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cust := CustomerID(42)
	reg.UpdateCampaigns([]CampaignData{
		{
			ID: CampaignID(1), Bid: 100, CustomerID: cust, CustomerBudget: 120,
			DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000,
		},
		{
			ID: CampaignID(2), Bid: 90, CustomerID: cust, CustomerBudget: 120,
			DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Weight: 2, Budget: 5000,
		},
	})

	_, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, int64(30), store.LoadCustomerBudget(reg.LoadShard(7).CustomerBudgetIndices[0]))
}

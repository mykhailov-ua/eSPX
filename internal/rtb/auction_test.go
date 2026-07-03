package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards second-price clearing and budget deduction on a small fixture.
func TestAuction_secondPrice_basic(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)

	c1 := CampaignID(1)
	c2 := CampaignID(2)
	c3 := CampaignID(3)

	campaigns := []CampaignData{
		{
			ID:           c1,
			Bid:          150,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       10,
			Budget:       1000,
		},
		{
			ID:           c2,
			Bid:          250,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       20,
			Budget:       2000,
		},
		{
			ID:           c3,
			Bid:          80,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       5,
			Budget:       1000,
		},
	}

	reg.UpdateCampaigns(campaigns)

	req := &BidRequest{
		DeviceType:   2,
		CategoryMask: 1,
		GeoHash:      10,
		MinBid:       100,
	}

	res, reason := reg.RunAuction(req)
	require.True(t, reason.OK())
	assert.Equal(t, c2, res.CampaignID)
	assert.Equal(t, int64(150), res.Price)
	assert.Equal(t, int64(1850), store.GetBudget(c2))
}

// Guards winner and clearing price when more than 128 campaigns qualify.
func TestAuction_secondPrice_manyCandidates(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 200
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           CampaignID(uint64(i + 1)),
			Bid:          int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   5,
			Weight:       uint32(i),
			Budget:       10000,
		}
	}

	reg.UpdateCampaigns(campaigns)

	res, reason := reg.RunAuction(stdReq(5, 50))
	require.True(t, reason.OK())
	assert.Equal(t, campaigns[n-1].ID, res.CampaignID)
	assert.Equal(t, int64(298), res.Price)
}

// Guards second-price clearing uses the shared floor when top bids tie.
func TestAuction_secondPrice_tiedTopFloors(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	c1, c2 := CampaignID(1), CampaignID(2)
	reg.UpdateCampaigns([]CampaignData{
		{ID: c1, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: c2, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	res, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, int64(200), res.Price)
}

// Guards equal bids pick the higher-weight campaign as winner.
func TestAuction_secondPrice_winnerTieBreak(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	c1, c2 := CampaignID(1), CampaignID(2)
	reg.UpdateCampaigns([]CampaignData{
		{ID: c1, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Weight: 1, Budget: 5000},
		{ID: c2, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Weight: 2, Budget: 5000},
	})

	res, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, c2, res.CampaignID)
}

// Guards equal bids and weights keep the first shard-order candidate.
func TestAuction_secondPrice_winnerTieBreak_equalWeight(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	c1, c2 := CampaignID(1), CampaignID(2)
	reg.UpdateCampaigns([]CampaignData{
		{ID: c1, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Weight: 5, Budget: 5000},
		{ID: c2, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Weight: 5, Budget: 5000},
	})

	res, reason := reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, c1, res.CampaignID)
}

// Guards second-price clearing when many candidates share the same bid floor.
func TestAuction_secondPrice_manyCandidates_equalFloors(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 150
	campaigns := make([]CampaignData, n)
	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID: CampaignID(uint64(i + 1)), Bid: 300, DeviceMask: 1, CategoryMask: 1,
			GeoHashVal: 5, Weight: uint32(i), Budget: 10000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	res, reason := reg.RunAuction(stdReq(5, 100))
	require.True(t, reason.OK())
	assert.Equal(t, int64(300), res.Price)
}

// Guards malformed requests are rejected without mutating campaign budgets.
func TestAuction_rejectsInvalidInput(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)

	c1 := CampaignID(1)
	reg.UpdateCampaigns([]CampaignData{{
		ID:           c1,
		Bid:          150,
		DeviceMask:   2,
		CategoryMask: 1,
		GeoHashVal:   10,
		Weight:       10,
		Budget:       1000,
	}})

	_, reason := reg.RunAuction(nil)
	assert.Equal(t, NoBidInvalidRequest, reason)

	_, reason = reg.RunAuction(&BidRequest{
		DeviceType:   2,
		CategoryMask: 1,
		GeoHash:      10,
		MinBid:       -500,
	})
	assert.Equal(t, NoBidInvalidRequest, reason)
	assert.Equal(t, int64(1000), store.GetBudget(c1))
}

// Guards no-bid reasons for empty catalog and exhausted budgets.
func TestAuction_noBidReasons(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)

	_, reason := reg.RunAuction(stdReq(7, 50))
	assert.Equal(t, NoBidEmptyShard, reason)

	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 10))
	_, reason = reg.RunAuction(stdReq(7, 50))
	assert.Equal(t, NoBidNoCandidates, reason)
}

// Guards shadow evaluation does not debit budgets.
func TestAuction_eval_noSpend(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)
	reg.UpdateCampaigns(singleCampaign(cid, 100, 500))

	req := stdReq(7, 50)
	res, reason := reg.RunAuctionEval(req)
	require.True(t, reason.OK())
	assert.Equal(t, cid, res.CampaignID)
	assert.Equal(t, int64(500), store.GetBudget(cid))
}

package rtb

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_rtb_deal_floor proves publisher floor (MinBid) blocks bids below clearing under concurrency.
func TestChaos_rtb_deal_floor(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	store := NewBudgetStore()
	reg := NewRegistry(store)
	geo := uint32(7)
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 200, DailyBudget: 10_000, PacingOpen: PacingOpen,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: geo, Budget: 10_000,
	}})

	reqLow := &BidRequest{GeoHash: geo, DeviceType: 1, CategoryMask: 1, MinBid: 100}
	res, reason := reg.RunAuctionEval(reqLow)
	require.True(t, reason.OK())
	assert.Equal(t, CampaignID(1), res.CampaignID)

	reqHigh := &BidRequest{GeoHash: geo, DeviceType: 1, CategoryMask: 1, MinBid: 500}
	_, reasonHigh := reg.RunAuctionEval(reqHigh)
	require.False(t, reasonHigh.OK())

	const workers = 24
	var wins, blocks atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(n int) {
			defer wg.Done()
			req := reqLow
			if n%2 == 0 {
				req = reqHigh
			}
			_, r := reg.RunAuctionEval(req)
			if r.OK() {
				wins.Add(1)
			} else {
				blocks.Add(1)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, uint64(workers/2), wins.Load())
	assert.Equal(t, uint64(workers/2), blocks.Load())

	logRtbChaosProof(t, "rtb_deal_floor", map[string]string{
		"subsystem":       "rtb_auction",
		"baseline_ok":     "true",
		"fault_type":      "elevated_publisher_floor",
		"workers":         "24",
		"floor_blocks":    itoaU64(blocks.Load()),
		"floor_clears":    itoaU64(wins.Load()),
		"min_bid_reject":  reasonHigh.String(),
	})
}

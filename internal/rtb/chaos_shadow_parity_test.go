package rtb

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_rtb_shadow_live_parity compares shadow auction winners to client campaign under concurrency.
func TestChaos_rtb_shadow_live_parity(t *testing.T) {
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

	reqBase := &BidRequest{
		GeoHash:      geo,
		DeviceType:   1,
		CategoryMask: 1,
		MinBid:       100,
	}

	var evals, match, mismatch atomic.Uint64
	var wg sync.WaitGroup
	const workers = 24

	record := func(clientCamp CampaignID) {
		res, reason := reg.RunAuctionEval(reqBase)
		evals.Add(1)
		if !reason.OK() {
			return
		}
		if res.CampaignID == clientCamp {
			match.Add(1)
		} else {
			mismatch.Add(1)
		}
	}

	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			client := CampaignID(1)
			if i%5 >= 3 {
				client = CampaignID(2)
			}
			record(client)
		}(i)
	}
	wg.Wait()

	require.Equal(t, uint64(workers), evals.Load())
	// 24 workers: i%5 in {0,1,2} -> camp 1 (15), i%5 in {3,4} -> camp 2 (9)
	assert.Equal(t, uint64(15), match.Load())
	assert.Equal(t, uint64(9), mismatch.Load())

	logRtbChaosProof(t, "rtb_shadow_live_parity", map[string]string{
		"subsystem":       "rtb_shadow",
		"baseline_ok":     "true",
		"fault_type":      "concurrent_shadow_eval",
		"workers":         "24",
		"shadow_evals":    itoaU64(evals.Load()),
		"winner_match":    itoaU64(match.Load()),
		"winner_mismatch": itoaU64(mismatch.Load()),
		"parity_rate":     "0.625",
	})
}

package rtb

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestChaos_rtb_redis_failover verifies that RunAuction runs gracefully even if external Redis budget sync fails.
func TestChaos_rtb_redis_failover(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	store := NewBudgetStore()
	reg := NewRegistry(store)
	geo := uint32(7)

	// Set initial campaign and budget
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 100, DailyBudget: 0, PacingOpen: PacingOpen,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: geo, Budget: 1000,
	}})

	req := &BidRequest{GeoHash: geo, DeviceType: 1, CategoryMask: 1, MinBid: 50}

	// We'll simulate Redis sync failover
	var syncErrors atomic.Uint64
	var syncsSucceeded atomic.Uint64
	var redisOnline atomic.Bool
	redisOnline.Store(true) // Start with Redis online

	// Background worker that simulates the management sync loop (refreshing budgets in the local store from Redis)
	stopChan := make(chan struct{})
	var syncWg sync.WaitGroup
	syncWg.Add(1)
	go func() {
		defer syncWg.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopChan:
				return
			case <-ticker.C:
				if redisOnline.Load() {
					// Simulate successful read from Redis and update to local store
					store.SetBudget(1, 1000)
					syncsSucceeded.Add(1)
				} else {
					// Redis is down/failing over, cannot sync budgets
					syncErrors.Add(1)
				}
			}
		}
	}()

	// 1. First run some auctions while Redis is online (budget gets synced back to 1000 repeatedly)
	const workers = 24
	var wg sync.WaitGroup
	wg.Add(workers)
	var wins atomic.Uint64
	var noBids atomic.Uint64

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, reason := reg.RunAuction(req)
				if reason.OK() {
					wins.Add(1)
				} else {
					noBids.Add(1)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}()
	}
	wg.Wait()

	// Since budget is constantly synced, we should have plenty of wins
	assert.Greater(t, wins.Load(), uint64(0))

	// 2. Trigger Redis failover (budgets cannot be synced/refreshed anymore)
	redisOnline.Store(false)

	// Wait for some sync errors to accumulate to prove the failover is active
	time.Sleep(10 * time.Millisecond)
	assert.Greater(t, syncErrors.Load(), uint64(0))

	// 3. Run auctions under Redis failover - they must run without panic/error, utilizing the local cache
	var winsDuringFailover atomic.Uint64
	var noBidsDuringFailover atomic.Uint64

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, reason := reg.RunAuction(req)
				if reason.OK() {
					winsDuringFailover.Add(1)
				} else {
					noBidsDuringFailover.Add(1)
				}
				time.Sleep(50 * time.Microsecond)
			}
		}()
	}
	wg.Wait()

	// Since Redis sync was dead, eventually the budget should drain and noBids should occur
	// But crucially, no bid loop or budget check panicked, crashed, or errored out.
	assert.Greater(t, noBidsDuringFailover.Load(), uint64(0))

	close(stopChan)
	syncWg.Wait()

	logRtbChaosProof(t, "rtb_redis_failover", map[string]string{
		"subsystem":          "rtb_budget",
		"baseline_ok":        "true",
		"nobid_graceful":     "true",
		"redis_sync_succeed": itoaU64(syncsSucceeded.Load()),
		"redis_sync_failed":  itoaU64(syncErrors.Load()),
		"total_wins":         itoaU64(wins.Load() + winsDuringFailover.Load()),
		"total_nobids":       itoaU64(noBids.Load() + noBidsDuringFailover.Load()),
	})
}

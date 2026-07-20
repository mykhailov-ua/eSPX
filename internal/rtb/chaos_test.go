package rtb

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chaosInstallShard swaps one geo shard with a handcrafted catalog for fault injection.
func chaosInstallShard(reg *Registry, geo uint32, shard *CampaignAuctionRegistry) {
	snap := reg.loadCatalog()
	var shards [geoShardCount]*CampaignAuctionRegistry
	if snap != nil {
		shards = snap.shards
	}
	shards[geo&geoShardMask] = shard
	reg.publishCatalog(shards)
}

// chaosRunAuction wraps RunAuction and reports whether the call panicked.
func chaosRunAuction(reg *Registry, req *BidRequest) (res AuctionResult, reason NoBidReason, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	res, reason = reg.RunAuction(req)
	return res, reason, false
}

func chaosBudgetTriple(store *BudgetStore, campIdx, custIdx uint32) (campaign, customer, daily int64) {
	campaign = store.LoadBudget(campIdx)
	customer = store.LoadCustomerBudget(custIdx)
	daily = store.loadOn(&store.dailySpent, campIdx)
	return campaign, customer, daily
}

// --- A: garbage input ---

func TestChaos_A1_nilRequest(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 1000))

	_, reason, panicked := chaosRunAuction(reg, nil)
	assert.False(t, panicked)
	assert.Equal(t, NoBidInvalidRequest, reason)
	assert.Equal(t, int64(1000), store.GetBudget(CampaignID(1)))
	logRtbChaosProof(t, "rtb_nil_request", map[string]string{"outcome": "invalid", "no_panic": "true"})
}

func TestChaos_A2_negativeMinBid(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 1000))

	_, reason, panicked := chaosRunAuction(reg, &BidRequest{
		DeviceType: 1, CategoryMask: 1, GeoHash: 7, MinBid: -1,
	})
	assert.False(t, panicked)
	assert.Equal(t, NoBidInvalidRequest, reason)
	logRtbChaosProof(t, "rtb_negative_min_bid", map[string]string{"outcome": "invalid"})
}

func TestChaos_A3_zeroDeviceMask(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 1000))

	_, reason, panicked := chaosRunAuction(reg, &BidRequest{
		DeviceType: 0, CategoryMask: 1, GeoHash: 7, MinBid: 50,
	})
	assert.False(t, panicked)
	assert.Equal(t, NoBidNoCandidates, reason)
	logRtbChaosProof(t, "rtb_zero_device", map[string]string{"outcome": "no_candidates"})
}

func TestChaos_A4_zeroCategoryMask(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 1000))

	_, reason, panicked := chaosRunAuction(reg, &BidRequest{
		DeviceType: 1, CategoryMask: 0, GeoHash: 7, MinBid: 50,
	})
	assert.False(t, panicked)
	assert.Equal(t, NoBidNoCandidates, reason)
	logRtbChaosProof(t, "rtb_zero_category", map[string]string{"outcome": "no_candidates"})
}

func TestChaos_A5_maxIntMinBid(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 1000))

	_, reason, panicked := chaosRunAuction(reg, &BidRequest{
		DeviceType: 1, CategoryMask: 1, GeoHash: 7, MinBid: math.MaxInt64,
	})
	assert.False(t, panicked)
	assert.False(t, reason.OK())
	logRtbChaosProof(t, "rtb_max_min_bid", map[string]string{"outcome": reason.String()})
}

func TestChaos_A6_emptyGeoShard(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 1000))

	_, reason, panicked := chaosRunAuction(reg, stdReq(9999, 50))
	assert.False(t, panicked)
	assert.True(t, reason == NoBidEmptyShard || reason == NoBidNoCandidates)
	logRtbChaosProof(t, "rtb_unknown_geo", map[string]string{"outcome": reason.String()})
}

// --- B: corrupt catalog ---

func TestChaos_B1_countExceedsSlices(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 1000))

	chaosInstallShard(reg, 7, &CampaignAuctionRegistry{
		Count:       5,
		CampaignIDs: []CampaignID{1},
		Bids:        []int64{100},
	})

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked, "corrupt count must not panic")
	assert.Equal(t, NoBidCorruptCatalog, reason)
	logRtbChaosProof(t, "rtb_count_gt_slices", map[string]string{"outcome": "corrupt_catalog"})
}

func TestChaos_B2_nonemptyCountEmptyGeoIndex(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	idx := store.GetOrAllocateSlot(CampaignID(1), 1000)

	chaosInstallShard(reg, 7, &CampaignAuctionRegistry{
		Count:                 1,
		CampaignIDs:           []CampaignID{1},
		Bids:                  []int64{100},
		CTRPPM:                []uint32{CTRPPMUnit},
		Reserves:              []int64{0},
		DailyBudgets:          []int64{0},
		PacingOpen:            []uint8{PacingOpen},
		DeviceMasks:           []uint8{1},
		CategoryMasks:         []uint64{1},
		GeoHashes:             []uint32{7},
		Weights:               []uint32{1},
		BudgetIndices:         []uint32{idx},
		CustomerBudgetIndices: []uint32{invalidCustomerBudgetIdx},
		GeoBucketCount:        0,
	})

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.Equal(t, NoBidNoCandidates, reason)
	logRtbChaosProof(t, "rtb_empty_geo_index", map[string]string{"outcome": "no_candidates"})
}

func TestChaos_B3_geoBucketIdxOutOfBounds(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	idx := store.GetOrAllocateSlot(CampaignID(1), 1000)

	chaosInstallShard(reg, 7, &CampaignAuctionRegistry{
		Count:                 1,
		CampaignIDs:           []CampaignID{1},
		Bids:                  []int64{100},
		CTRPPM:                []uint32{CTRPPMUnit},
		Reserves:              []int64{0},
		DailyBudgets:          []int64{0},
		PacingOpen:            []uint8{PacingOpen},
		DeviceMasks:           []uint8{1},
		CategoryMasks:         []uint64{1},
		GeoHashes:             []uint32{7},
		Weights:               []uint32{1},
		BudgetIndices:         []uint32{idx},
		CustomerBudgetIndices: []uint32{invalidCustomerBudgetIdx},
		GeoBucketCount:        1,
		GeoBucketHash:         []uint32{7},
		GeoBucketStart:        []uint32{0, 1},
		GeoBucketSoA: candidateBucketSoA{
			CatalogIdx:            []uint32{99},
			Bids:                  []int64{100},
			CTRPPM:                []uint32{CTRPPMUnit},
			Reserves:              []int64{0},
			DailyBudgets:          []int64{0},
			PacingOpen:            []uint8{PacingOpen},
			DeviceMasks:           []uint8{1},
			CategoryMasks:         []uint64{1},
			Weights:               []uint32{1},
			BudgetIndices:         []uint32{idx},
			CustomerBudgetIndices: []uint32{invalidCustomerBudgetIdx},
		},
	})

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked, "OOB geo bucket index must not panic")
	assert.Equal(t, NoBidCorruptCatalog, reason)
	logRtbChaosProof(t, "rtb_oob_geo_bucket_idx", map[string]string{"outcome": "corrupt_catalog"})
}

func TestChaos_B4_budgetIndexOutOfRange(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)

	chaosInstallShard(reg, 7, &CampaignAuctionRegistry{
		Count:                 1,
		CampaignIDs:           []CampaignID{1},
		Bids:                  []int64{100},
		CTRPPM:                []uint32{CTRPPMUnit},
		Reserves:              []int64{0},
		DailyBudgets:          []int64{0},
		PacingOpen:            []uint8{PacingOpen},
		DeviceMasks:           []uint8{1},
		CategoryMasks:         []uint64{1},
		GeoHashes:             []uint32{7},
		Weights:               []uint32{1},
		BudgetIndices:         []uint32{99999},
		CustomerBudgetIndices: []uint32{invalidCustomerBudgetIdx},
	})
	buildGeoIndex(reg.LoadShard(7))

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.Equal(t, NoBidCorruptCatalog, reason)
	logRtbChaosProof(t, "rtb_oob_budget_idx", map[string]string{"outcome": "corrupt_catalog", "no_debit": "true"})
}

func TestChaos_B5_negativeBidInCatalog(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{{
		ID: CampaignID(1), Bid: -50, DeviceMask: 1, CategoryMask: 1,
		GeoHashVal: 7, Budget: 1000,
	}})

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 0))
	assert.False(t, panicked)
	assert.False(t, reason.OK())
	logRtbChaosProof(t, "rtb_negative_bid", map[string]string{"outcome": reason.String()})
}

// --- C: spend ordering ---

func TestChaos_C1_zeroCampaignBudget(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 0))

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.Equal(t, NoBidNoCandidates, reason)
	logRtbChaosProof(t, "rtb_zero_budget", map[string]string{"outcome": "no_candidates"})
}

func TestChaos_C2_customerRollbackOnSpendFail(t *testing.T) {
	store := NewBudgetStore()
	campIdx := store.GetOrAllocateSlot(CampaignID(1), 1000)
	custIdx := store.GetOrAllocateCustomerSlot(CustomerID(9), 50)

	beforeCamp, beforeCust, _ := chaosBudgetTriple(store, campIdx, custIdx)
	ok := store.CheckAndSpendAll(campIdx, custIdx, 100, 0)
	assert.False(t, ok)

	afterCamp, afterCust, _ := chaosBudgetTriple(store, campIdx, custIdx)
	assert.Equal(t, beforeCamp, afterCamp, "campaign budget must rollback")
	assert.Equal(t, beforeCust, afterCust, "customer budget must rollback")
	logRtbChaosProof(t, "rtb_customer_insufficient", map[string]string{"rollback": "true"})
}

func TestChaos_C3_dailyRollbackOnSpendFail(t *testing.T) {
	store := NewBudgetStore()
	campIdx := store.GetOrAllocateSlot(CampaignID(1), 1000)
	custIdx := store.GetOrAllocateCustomerSlot(CustomerID(9), 500)

	beforeCamp, beforeCust, beforeDaily := chaosBudgetTriple(store, campIdx, custIdx)
	ok := store.CheckAndSpendAll(campIdx, custIdx, 100, 50)
	assert.False(t, ok)

	afterCamp, afterCust, afterDaily := chaosBudgetTriple(store, campIdx, custIdx)
	assert.Equal(t, beforeCamp, afterCamp)
	assert.Equal(t, beforeCust, afterCust)
	assert.Equal(t, beforeDaily, afterDaily)
	logRtbChaosProof(t, "rtb_daily_cap_on_spend", map[string]string{"rollback": "all"})
}

func TestChaos_C4_spendOutOfRangeIndex(t *testing.T) {
	store := NewBudgetStore()
	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		_ = store.CheckAndSpendAll(99999, invalidCustomerBudgetIdx, 10, 0)
	}()
	assert.False(t, panicked)
	logRtbChaosProof(t, "rtb_oob_spend_idx", map[string]string{"no_panic": "true"})
}

func TestChaos_C5_exactBudgetSingleWin(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 100))

	res1, r1, _ := chaosRunAuction(reg, stdReq(7, 50))
	require.True(t, r1.OK())
	assert.Equal(t, int64(50), res1.Price)
	assert.Equal(t, int64(50), store.GetBudget(CampaignID(1)))

	_, r2, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.Equal(t, NoBidNoCandidates, r2)
	logRtbChaosProof(t, "rtb_exact_one_clearing", map[string]string{
		"first_price": strconv.FormatInt(res1.Price, 10),
		"second":      r2.String(),
	})
}

func TestChaos_C6_clearingPriceNotAboveBid(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: 2, Bid: 150, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	res, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	require.False(t, panicked)
	require.True(t, reason.OK())
	assert.LessOrEqual(t, res.Price, int64(200))
	logRtbChaosProof(t, "rtb_clearing_price_cap", map[string]string{
		"clearing_price": strconv.FormatInt(res.Price, 10),
		"winner_bid":     "200",
	})
}

func TestChaos_C7_setBudgetZeroRace(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)
	reg.UpdateCampaigns(singleCampaign(cid, 100, 200))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				store.SetBudget(cid, 0)
			}
		}
	}()

	for i := 0; i < 200; i++ {
		reg.RunAuction(stdReq(7, 50))
	}
	close(stop)
	wg.Wait()

	assert.GreaterOrEqual(t, store.GetBudget(cid), int64(0))
	logRtbChaosProof(t, "rtb_concurrent_zero_budget", map[string]string{"min_budget_gte": "0"})
}

// --- D: NoBid priority ---

func TestChaos_D1_allPacingClosed(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{{
		ID: CampaignID(1), Bid: 100, PacingOpen: PacingClosed,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 1000,
	}})

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.Equal(t, NoBidPacingClosed, reason)
	logRtbChaosProof(t, "rtb_all_pacing_closed", nil)
}

func TestChaos_D2_pacingBeatsDaily(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 100, PacingOpen: PacingClosed, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 1000},
		{ID: 2, Bid: 100, DailyBudget: 10, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 1000},
	})
	idx := reg.LoadShard(7).BudgetIndices[1]
	store.addDailySpendLocked(idx, 10)

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.Equal(t, NoBidPacingClosed, reason)
	logRtbChaosProof(t, "rtb_pacing_over_daily", map[string]string{"priority": "pacing"})
}

func TestChaos_D3_dailyOnlyBlocked(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 100, DailyBudget: 50, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 1000,
	}})
	idx := reg.LoadShard(7).BudgetIndices[0]
	store.addDailySpendLocked(idx, 60)

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.Equal(t, NoBidDailyCapExceeded, reason)
	logRtbChaosProof(t, "rtb_daily_only_blocked", nil)
}

// --- E: clearing edges ---

func TestChaos_E1_reserveAboveSecondPrice(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 200, Reserve: 180, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: 2, Bid: 120, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	res, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	require.False(t, panicked)
	require.True(t, reason.OK())
	assert.Equal(t, int64(180), res.Price)
	logRtbChaosProof(t, "rtb_reserve_lifts_clearing", map[string]string{"price": "180"})
}

func TestChaos_E2_firstPriceWithReserve(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.SetClearingMode(ClearingFirstPrice)
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 200, Reserve: 150, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000,
	}})

	res, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	require.False(t, panicked)
	require.True(t, reason.OK())
	assert.Equal(t, int64(200), res.Price)
	logRtbChaosProof(t, "rtb_first_price_reserve", map[string]string{"winner_bid": "200"})
}

func TestChaos_E3_zeroCTRPNormalized(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 100, CTRPPM: 0, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 1000,
	}})

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.True(t, reason.OK())
	logRtbChaosProof(t, "rtb_zero_ctr_normalized", map[string]string{"outcome": "ok"})
}

// --- F: customer pool ---

func TestChaos_F1_sharedCustomerDrains(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cust := CustomerID(42)
	reg.UpdateCampaigns([]CampaignData{
		{ID: 1, Bid: 100, CustomerID: cust, CustomerBudget: 120, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
		{ID: 2, Bid: 90, CustomerID: cust, CustomerBudget: 120, Weight: 2, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 5000},
	})

	_, r1, _ := chaosRunAuction(reg, stdReq(7, 50))
	require.True(t, r1.OK())
	custIdx := reg.LoadShard(7).CustomerBudgetIndices[0]
	assert.Equal(t, int64(30), store.LoadCustomerBudget(custIdx))

	_, r2, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.False(t, r2.OK())
	logRtbChaosProof(t, "rtb_shared_customer", map[string]string{"second_auction": "no_bid"})
}

func TestChaos_F2_zeroCustomerIDDisabled(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 100, CustomerID: 0, CustomerBudget: 10,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 1000,
	}})

	sh := reg.LoadShard(7)
	assert.Equal(t, invalidCustomerBudgetIdx, sh.CustomerBudgetIndices[0])

	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.True(t, reason.OK())
	logRtbChaosProof(t, "rtb_customer_id_zero", map[string]string{"ignores_customer_pool": "true"})
}

func TestChaos_F3_customerExhaustedNoCampaignDebit(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cust := CustomerID(7)
	reg.UpdateCampaigns([]CampaignData{{
		ID: 1, Bid: 100, CustomerID: cust, CustomerBudget: 40,
		DeviceMask: 1, CategoryMask: 1, GeoHashVal: 7, Budget: 1000,
	}})

	before := store.GetBudget(CampaignID(1))
	_, reason, panicked := chaosRunAuction(reg, stdReq(7, 50))
	assert.False(t, panicked)
	assert.Equal(t, NoBidNoCandidates, reason)
	assert.Equal(t, before, store.GetBudget(CampaignID(1)))
	logRtbChaosProof(t, "rtb_customer_low_prefilter", map[string]string{"no_campaign_debit": "true"})
}

// --- H: concurrency ---

func TestChaos_H1_catalogClearDuringAuction(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 10_000))

	var panicked atomic.Bool
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				func() {
					defer func() {
						if recover() != nil {
							panicked.Store(true)
						}
					}()
					reg.RunAuction(stdReq(7, 50))
				}()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			reg.UpdateCampaigns(nil)
			reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 10_000))
		}
		close(stop)
	}()

	wg.Wait()
	assert.False(t, panicked.Load())
	assert.GreaterOrEqual(t, store.GetBudget(CampaignID(1)), int64(0))
	logRtbChaosProof(t, "rtb_catalog_rebuild_during_auction", map[string]string{"no_panic": "true"})
}

func TestChaos_H2_parallelDrainNonNegative(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(1), 100, 100))

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				reg.RunAuction(stdReq(7, 50))
			}
		}()
	}
	wg.Wait()

	assert.GreaterOrEqual(t, store.GetBudget(CampaignID(1)), int64(0))
	logRtbChaosProof(t, "rtb_parallel_drain", map[string]string{
		"remaining": strconv.FormatInt(store.GetBudget(CampaignID(1)), 10),
	})
}

package rtb

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards UpdateCampaigns does not overwrite live budget for an existing campaign slot.
func TestRegistry_updateCampaigns_preservesLiveBudget(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)

	reg.UpdateCampaigns(singleCampaign(cid, 100, 1000))

	req := stdReq(7, 50)
	for i := 0; i < 9; i++ {
		_, reason := reg.RunAuction(req)
		require.True(t, reason.OK(), "auction %d", i)
	}
	assert.Equal(t, int64(550), store.GetBudget(cid), "after 9 second-price clears at MinBid")

	reg.UpdateCampaigns(singleCampaign(cid, 100, 9999))
	assert.Equal(t, int64(550), store.GetBudget(cid))
}

// Guards SetBudget and LoadSnapshot cannot corrupt another campaign slot index.
func TestRegistry_setBudget_loadSnapshot_noCrossWrite(t *testing.T) {
	storeA := NewBudgetStore()
	regA := NewRegistry(storeA)

	idA := CampaignID(10)
	regA.UpdateCampaigns(singleCampaign(idA, 100, 1000))

	tmpDir, err := os.MkdirTemp("", "rtb-snap-race-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	snapPath := filepath.Join(tmpDir, "snap.bin")
	require.NoError(t, regA.SaveSnapshot(snapPath))

	var crossWrites atomic.Int64
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
				_ = regA.LoadSnapshot(snapPath)
			}
		}
	}()

	idB := CampaignID(11)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				storeA.SetBudget(idA, 42)
				if storeA.GetBudget(idB) == 42 {
					crossWrites.Add(1)
				}
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	assert.Zero(t, crossWrites.Load())
}

// Guards SaveSnapshot captures catalog and budget slots under one generation.
func TestRegistry_saveSnapshot_consistentUnderConcurrentSpend(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)
	reg.UpdateCampaigns(singleCampaign(cid, 100, 1000))

	tmpDir, err := os.MkdirTemp("", "rtb-snap-pit-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	snapPath := filepath.Join(tmpDir, "snap.bin")

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
				_, _ = reg.RunAuction(stdReq(7, 50))
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = reg.SaveSnapshot(snapPath)
		}
	}()

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	restored := NewBudgetStore()
	restReg := NewRegistry(restored)
	require.NoError(t, restReg.LoadSnapshot(snapPath))

	liveBudget := store.GetBudget(cid)
	restoredBudget := restored.GetBudget(cid)
	_, restInCatalog := catalogIDs(restReg)[cid]

	if liveBudget < 1000 && restoredBudget == 1000 && restInCatalog {
		t.Fatalf("snapshot shows full budget %d but live is %d", restoredBudget, liveBudget)
	}
}

// Guards RCU-pinned auctions never drive budget negative after catalog removal.
func TestRegistry_runAuction_ghostWinnerBudgetNonNegative(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)
	reg.UpdateCampaigns(singleCampaign(cid, 100, 1_000_000))

	var ghostWins atomic.Int64
	var totalWins atomic.Int64
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
				res, reason := reg.RunAuction(stdReq(7, 50))
				if !reason.OK() {
					continue
				}
				totalWins.Add(1)
				if _, live := catalogIDs(reg)[res.CampaignID]; !live {
					ghostWins.Add(1)
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if i%2 == 0 {
				reg.UpdateCampaigns(nil)
			} else {
				reg.UpdateCampaigns(singleCampaign(cid, 100, 1_000_000))
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	t.Logf("ghost wins: %d / %d total", ghostWins.Load(), totalWins.Load())
	assert.GreaterOrEqual(t, store.GetBudget(cid), int64(0))
}

// Guards auctions fail after the catalog is emptied without spending budget.
func TestRegistry_runAuction_emptyCatalogNoSpend(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)
	reg.UpdateCampaigns(singleCampaign(cid, 100, 1000))

	reg.UpdateCampaigns(nil)
	_, reason := reg.RunAuction(stdReq(7, 50))
	assert.False(t, reason.OK())
	assert.Equal(t, int64(1000), store.GetBudget(cid))
}

// Guards concurrent winners are bounded by clearing price and CAS.
func TestRegistry_runAuction_concurrentSpendBoundedByCAS(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)
	reg.UpdateCampaigns(singleCampaign(cid, 100, 120))

	req := stdReq(7, 50)
	var wins atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, reason := reg.RunAuction(req); reason.OK() {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()

	w := wins.Load()
	assert.GreaterOrEqual(t, w, int64(1))
	assert.LessOrEqual(t, w, int64(2))
	assert.Equal(t, int64(120-50*w), store.GetBudget(cid))
}

// Guards concurrent bidding alongside shard rebuilds without budget overspend.
func TestRegistry_runAuction_concurrentCatalogRebuild(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 100
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           CampaignID(uint64(i + 1)),
			Bid:          int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   uint32(i % 16),
			Weight:       uint32(i),
			Budget:       5000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	var wg sync.WaitGroup
	workers := 12
	iterations := 500

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(workerID)))
			for i := 0; i < iterations; i++ {
				if i%50 == 0 {
					time.Sleep(time.Duration(rnd.Intn(5)+1) * time.Microsecond)
				}
				req := &BidRequest{
					DeviceType:   1,
					CategoryMask: 1,
					GeoHash:      uint32(rnd.Intn(geoShardCount)),
					MinBid:       int64(100 + rnd.Intn(40)),
				}
				_, _ = reg.RunAuction(req)
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		rnd := rand.New(rand.NewSource(999))
		for i := 0; i < 20; i++ {
			time.Sleep(time.Duration(rnd.Intn(8)+2) * time.Millisecond)

			updated := make([]CampaignData, n)
			for j := 0; j < n; j++ {
				updated[j] = campaigns[j]
				updated[j].Weight += uint32(i)
			}
			reg.UpdateCampaigns(updated)
		}
	}()

	wg.Wait()

	for i := 0; i < n; i++ {
		b := store.GetBudget(campaigns[i].ID)
		assert.GreaterOrEqual(t, b, int64(0), "campaign %d", campaigns[i].ID)
	}
}

// Guards corrupt shard Count values abort the auction safely.
func TestRegistry_runAuction_rejectsCorruptCount(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	reg.UpdateCampaigns(singleCampaign(CampaignID(42), 100, 1000))

	sh := reg.LoadShard(7)
	require.NotNil(t, sh)
	sh.Count = 9999

	_, reason := reg.RunAuction(stdReq(7, 50))
	assert.Equal(t, NoBidCorruptCatalog, reason)
}

// Guards out-of-range budget indices abort the auction safely.
func TestRegistry_runAuction_rejectsBadBudgetIndex(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)
	reg.UpdateCampaigns(singleCampaign(cid, 100, 1000))

	sh := reg.LoadShard(7)
	require.NotNil(t, sh)
	sh.BudgetIndices[0] = 99999

	_, reason := reg.RunAuction(stdReq(7, 50))
	assert.False(t, reason.OK())
}

// Guards readers never observe mixed catalog generations across shards.
func TestRegistry_updateCampaigns_atomicCatalogPublish(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)

	oldID := CampaignID(100)
	newID := CampaignID(200)
	reg.UpdateCampaigns(singleCampaign(oldID, 100, 5000))

	campaigns := []CampaignData{
		{ID: newID, Bid: 200, DeviceMask: 1, CategoryMask: 1, GeoHashVal: 0, Budget: 5000},
	}

	var sawMixed atomic.Bool
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
				snap := reg.loadCatalog()
				if snap == nil {
					continue
				}
				s0 := snap.shards[0]
				s7 := snap.shards[7]
				if s0 == nil || s7 == nil {
					continue
				}
				hasNew := false
				hasOld := false
				for j := 0; j < s0.Count && j < len(s0.CampaignIDs); j++ {
					if s0.CampaignIDs[j] == newID {
						hasNew = true
					}
				}
				for j := 0; j < s7.Count && j < len(s7.CampaignIDs); j++ {
					if s7.CampaignIDs[j] == oldID {
						hasOld = true
					}
				}
				if hasNew && hasOld {
					sawMixed.Store(true)
				}
			}
		}
	}()

	for i := 0; i < 500; i++ {
		reg.UpdateCampaigns(campaigns)
		reg.UpdateCampaigns(singleCampaign(oldID, 100, 5000))
	}
	close(stop)
	wg.Wait()

	assert.False(t, sawMixed.Load())
}

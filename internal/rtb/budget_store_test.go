package rtb

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards SetBudget stays visible while GetOrAllocateSlot grows the backing slice.
func TestBudgetStore_setBudget_concurrentSlotGrowth(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)

	const n = 32
	ids := make([]CampaignID, n)
	campaigns := make([]CampaignData, n)
	for i := 0; i < n; i++ {
		ids[i] = CampaignID(uint64(i + 1))
		campaigns[i] = CampaignData{
			ID: ids[i], Bid: 10, DeviceMask: 1, CategoryMask: 1,
			GeoHashVal: uint32(i % geoShardCount), Weight: 1, Budget: 500,
		}
	}
	reg.UpdateCampaigns(campaigns)

	target := ids[0]
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
				extra := CampaignData{
					ID: CampaignID(9999), Bid: 10, DeviceMask: 1, CategoryMask: 1,
					GeoHashVal: 3, Weight: 1, Budget: 1,
				}
				reg.UpdateCampaigns(append(campaigns, extra))
			}
		}
	}()

	for i := 0; i < 5000; i++ {
		store.SetBudget(target, 500)
	}
	close(stop)
	wg.Wait()

	assert.Equal(t, int64(500), store.GetBudget(target))
}

// Guards negative SetBudget values are clamped to zero.
func TestBudgetStore_setBudget_clampsNegative(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	cid := CampaignID(1)
	reg.UpdateCampaigns(singleCampaign(cid, 100, 1000))
	store.SetBudget(cid, -500)

	assert.Equal(t, int64(0), store.GetBudget(cid))
	_, reason := reg.RunAuction(stdReq(7, 50))
	assert.Equal(t, NoBidNoCandidates, reason)

	store.SetBudget(cid, 100)
	_, reason = reg.RunAuction(stdReq(7, 50))
	require.True(t, reason.OK())
	assert.Equal(t, int64(50), store.GetBudget(cid))

	store.SetBudget(cid, -10)
	assert.Equal(t, int64(0), store.GetBudget(cid))
	spent := store.CheckAndSpend(0, 50)
	assert.False(t, spent)
}

// Guards GetBudget stays in bounds while LoadSnapshot races SetBudget.
func TestBudgetStore_getBudget_noPanicUnderSnapshotRace(t *testing.T) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	id := CampaignID(7)
	reg.UpdateCampaigns(singleCampaign(id, 100, 1000))

	tmpDir, err := os.MkdirTemp("", "rtb-budget-panic-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	snapPath := filepath.Join(tmpDir, "snap.bin")
	require.NoError(t, reg.SaveSnapshot(snapPath))

	panicked := make(chan struct{}, 1)
	go func() {
		defer func() {
			if recover() != nil {
				panicked <- struct{}{}
			}
		}()
		for i := 0; i < 1000; i++ {
			_ = reg.LoadSnapshot(snapPath)
			store.SetBudget(id, int64(i))
			_ = store.GetBudget(id)
		}
	}()

	select {
	case <-panicked:
		t.Fatal("GetBudget panicked under LoadSnapshot race")
	case <-time.After(300 * time.Millisecond):
	}
}

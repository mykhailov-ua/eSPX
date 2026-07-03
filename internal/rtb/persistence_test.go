package rtb

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards round-trip persistence keeps budgets and every shard column intact.
func TestRegistry_snapshot_roundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "espx-rtb-snap-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	snapPath := filepath.Join(tmpDir, "snapshot.bin")

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
			GeoHashVal:   11,
			Weight:       5,
			Budget:       1500,
		},
	}

	reg.UpdateCampaigns(campaigns)

	b1 := store.GetBudget(c1)
	b2 := store.GetBudget(c2)
	b3 := store.GetBudget(c3)

	assert.Equal(t, int64(1000), b1)
	assert.Equal(t, int64(2000), b2)
	assert.Equal(t, int64(1500), b3)

	err = reg.SaveSnapshot(snapPath)
	require.NoError(t, err)

	_, err = os.Stat(snapPath)
	require.NoError(t, err)

	newStore := NewBudgetStore()
	newReg := NewRegistry(newStore)

	err = newReg.LoadSnapshot(snapPath)
	require.NoError(t, err)

	assert.Equal(t, b1, newStore.GetBudget(c1))
	assert.Equal(t, b2, newStore.GetBudget(c2))
	assert.Equal(t, b3, newStore.GetBudget(c3))

	for i := uint32(0); i < geoShardCount; i++ {
		origShard := reg.LoadShard(i)
		newShard := newReg.LoadShard(i)

		if origShard == nil || origShard.Count == 0 {
			assert.True(t, newShard == nil || newShard.Count == 0)
			continue
		}

		assert.Equal(t, origShard.Count, newShard.Count)
		assert.Equal(t, origShard.CampaignIDs, newShard.CampaignIDs)
		assert.Equal(t, origShard.Bids, newShard.Bids)
		assert.Equal(t, origShard.DeviceMasks, newShard.DeviceMasks)
		assert.Equal(t, origShard.CategoryMasks, newShard.CategoryMasks)
		assert.Equal(t, origShard.GeoHashes, newShard.GeoHashes)
		assert.Equal(t, origShard.Weights, newShard.Weights)
		assert.Equal(t, origShard.BudgetIndices, newShard.BudgetIndices)
	}
}

// Guards periodic snapshots and shutdown flush produce a restorable registry file.
func TestRegistry_startPersistence_periodicFlush(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "espx-rtb-persist-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	snapPath := filepath.Join(tmpDir, "snapshot.bin")

	store := NewBudgetStore()
	reg := NewRegistry(store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = reg.StartPersistence(ctx, snapPath, 100*time.Millisecond)
	require.NoError(t, err)

	c1 := CampaignID(1)
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
	}
	reg.UpdateCampaigns(campaigns)

	time.Sleep(250 * time.Millisecond)

	cancel()

	time.Sleep(50 * time.Millisecond)

	_, err = os.Stat(snapPath)
	require.NoError(t, err)

	newStore := NewBudgetStore()
	newReg := NewRegistry(newStore)

	ctx2, cancel2 := context.WithCancel(context.Background())

	err = newReg.StartPersistence(ctx2, snapPath, 100*time.Millisecond)
	require.NoError(t, err)

	assert.Equal(t, int64(1000), newStore.GetBudget(c1))

	cancel2()
	time.Sleep(100 * time.Millisecond)
}

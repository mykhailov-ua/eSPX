package rtb

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotSaveAndLoad(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "espx-rtb-snap-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	snapPath := filepath.Join(tmpDir, "snapshot.bin")

	store := NewBudgetStore()
	reg := NewRegistry(store)

	c1 := uuid.New()
	c2 := uuid.New()
	c3 := uuid.New()

	campaigns := []CampaignData{
		{
			ID:           c1,
			BidFloor:     150,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       10,
			Budget:       1000,
		},
		{
			ID:           c2,
			BidFloor:     250,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   10,
			Weight:       20,
			Budget:       2000,
		},
		{
			ID:           c3,
			BidFloor:     80,
			DeviceMask:   2,
			CategoryMask: 1,
			GeoHashVal:   11, // different shard
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

	for i := uint32(0); i < 16; i++ {
		origShard := reg.LoadShard(i)
		newShard := newReg.LoadShard(i)

		if origShard == nil || origShard.Count == 0 {
			assert.True(t, newShard == nil || newShard.Count == 0)
			continue
		}

		assert.Equal(t, origShard.Count, newShard.Count)
		assert.Equal(t, origShard.CampaignIDs, newShard.CampaignIDs)
		assert.Equal(t, origShard.BidFloors, newShard.BidFloors)
		assert.Equal(t, origShard.DeviceMasks, newShard.DeviceMasks)
		assert.Equal(t, origShard.CategoryMasks, newShard.CategoryMasks)
		assert.Equal(t, origShard.GeoHashes, newShard.GeoHashes)
		assert.Equal(t, origShard.Weights, newShard.Weights)
		assert.Equal(t, origShard.BudgetIndices, newShard.BudgetIndices)
	}
}

func TestStartPersistence(t *testing.T) {
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

	c1 := uuid.New()
	campaigns := []CampaignData{
		{
			ID:           c1,
			BidFloor:     150,
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

func BenchmarkSaveSnapshot(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "espx-rtb-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	snapPath := filepath.Join(tmpDir, "snapshot.bin")

	store := NewBudgetStore()
	reg := NewRegistry(store)

	n := 10000
	campaigns := make([]CampaignData, n)
	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           uuid.New(),
			BidFloor:     int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   uint32(i % 16),
			Weight:       uint32(i),
			Budget:       10000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := reg.SaveSnapshot(snapPath)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLoadSnapshot(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "espx-rtb-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	snapPath := filepath.Join(tmpDir, "snapshot.bin")

	store := NewBudgetStore()
	reg := NewRegistry(store)

	n := 10000
	campaigns := make([]CampaignData, n)
	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           uuid.New(),
			BidFloor:     int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   uint32(i % 16),
			Weight:       uint32(i),
			Budget:       10000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	err = reg.SaveSnapshot(snapPath)
	if err != nil {
		b.Fatal(err)
	}

	newStore := NewBudgetStore()
	newReg := NewRegistry(newStore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := newReg.LoadSnapshot(snapPath)
		if err != nil {
			b.Fatal(err)
		}
	}
}

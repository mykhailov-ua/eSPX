package rtb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tracks cold-path snapshot write cost at production-scale campaign counts.
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
			ID:           CampaignID(uint64(i + 1)),
			Bid:          int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   uint32(i % geoShardCount),
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

// Tracks startup restore cost from a production-scale snapshot file.
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
			ID:           CampaignID(uint64(i + 1)),
			Bid:          int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   uint32(i % geoShardCount),
			Weight:       uint32(i),
			Budget:       10000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	err = reg.SaveSnapshot(snapPath)
	require.NoError(b, err)

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

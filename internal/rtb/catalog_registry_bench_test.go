package rtb

import "testing"

// Tracks cold-path cost of rebuilding every shard from a full campaign sync.
func BenchmarkUpdateCampaigns(b *testing.B) {
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 1000
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		campaigns[i] = CampaignData{
			ID:           CampaignID(uint64(i + 1)),
			Bid:          int64(100 + i),
			DeviceMask:   1,
			CategoryMask: 1,
			GeoHashVal:   uint32(i),
			Weight:       uint32(i),
			Budget:       10000,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reg.UpdateCampaigns(campaigns)
	}
}

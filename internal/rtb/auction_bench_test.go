package rtb

import "testing"

// Tracks hot-path auction latency with campaigns spread across geo shards.
func BenchmarkAuction(b *testing.B) {
	SetMetricsEnabled(false)
	store := NewBudgetStore()
	reg := NewRegistry(store)
	n := 1000
	campaigns := make([]CampaignData, n)

	for i := 0; i < n; i++ {
		deviceMask := uint8(1 << (i % 3))
		campaigns[i] = CampaignData{
			ID:           CampaignID(uint64(i + 1)),
			Bid:          int64(100 + (i % 500)),
			DeviceMask:   deviceMask,
			CategoryMask: uint64(1 << (i % 8)),
			GeoHashVal:   uint32(i),
			Weight:       uint32(i),
			Budget:       1000000000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	req := &BidRequest{
		DeviceType:   2,
		CategoryMask: 4,
		GeoHash:      2,
		MinBid:       150,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.RunAuction(req)
	}
}

// Tracks worst-case scan cost when many campaigns share one geo shard.
func BenchmarkAuction_highDensity(b *testing.B) {
	SetMetricsEnabled(false)
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
			GeoHashVal:   5,
			Weight:       uint32(i),
			Budget:       1000000000,
		}
	}
	reg.UpdateCampaigns(campaigns)

	req := &BidRequest{
		DeviceType:   1,
		CategoryMask: 1,
		GeoHash:      5,
		MinBid:       50,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.RunAuction(req)
	}
}

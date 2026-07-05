package rtb

import (
	"strings"
	"testing"
)

// logRtbChaosProof emits chaos_proof lines parsed by scripts/chaos/test_chaos.sh.
func logRtbChaosProof(t *testing.T, fault string, kv map[string]string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("chaos_proof fault=")
	b.WriteString(fault)
	for k, v := range kv {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
	}
	t.Log(b.String())
}

// Builds a one-campaign UpdateCampaigns payload for tests.
func singleCampaign(id CampaignID, bid int64, budget int64) []CampaignData {
	return []CampaignData{{
		ID:           id,
		Bid:          bid,
		DeviceMask:   1,
		CategoryMask: 1,
		GeoHashVal:   7,
		Weight:       1,
		Budget:       budget,
	}}
}

// Builds a bid request with the common targeting fields used in lifecycle tests.
func stdReq(geo uint32, minBid int64) *BidRequest {
	return &BidRequest{
		DeviceType:   1,
		CategoryMask: 1,
		GeoHash:      geo,
		MinBid:       minBid,
	}
}

// Builds the set of campaign IDs present in one catalog snapshot.
func catalogIDs(reg *Registry) map[CampaignID]struct{} {
	out := make(map[CampaignID]struct{})
	snap := reg.loadCatalog()
	if snap == nil {
		return out
	}
	for i := 0; i < geoShardCount; i++ {
		sh := snap.shards[i]
		if sh == nil || sh.Count == 0 {
			continue
		}
		for j := 0; j < sh.Count && j < len(sh.CampaignIDs); j++ {
			out[sh.CampaignIDs[j]] = struct{}{}
		}
	}
	return out
}

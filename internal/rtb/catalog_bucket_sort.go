package rtb

import "sort"

func sortBucketCandidates(reg *CampaignAuctionRegistry, bucket []uint32, start, end int) {
	if reg == nil || end-start < 2 {
		return
	}
	sort.Slice(bucket[start:end], func(a, b int) bool {
		ia := int(bucket[start+a])
		ib := int(bucket[start+b])
		sa := effectiveScore(reg.Bids[ia], reg.CTRPPM[ia])
		sb := effectiveScore(reg.Bids[ib], reg.CTRPPM[ib])
		if sa != sb {
			return sa > sb
		}
		if reg.Weights[ia] != reg.Weights[ib] {
			return reg.Weights[ia] > reg.Weights[ib]
		}
		return reg.Bids[ia] > reg.Bids[ib]
	})
}

func sortRegistryBuckets(reg *CampaignAuctionRegistry) {
	if reg == nil || reg.GeoBucketCount == 0 {
		return
	}
	for i := 0; i < reg.GeoBucketCount; i++ {
		start := int(reg.GeoBucketStart[i])
		end := int(reg.GeoBucketStart[i+1])
		sortBucketCandidates(reg, reg.GeoBucketIdx, start, end)
	}
	if reg.TargetBucketCount == 0 {
		return
	}
	for i := 0; i < reg.TargetBucketCount; i++ {
		start := int(reg.TargetBucketStart[i])
		end := int(reg.TargetBucketStart[i+1])
		sortBucketCandidates(reg, reg.TargetBucketIdx, start, end)
	}
}

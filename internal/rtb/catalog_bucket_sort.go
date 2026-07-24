package rtb

import "sort"

type bucketSoASorter struct {
	soa   *candidateBucketSoA
	start int
	end   int
}

func (s bucketSoASorter) Len() int { return s.end - s.start }

func (s bucketSoASorter) Swap(i, j int) {
	swapBucketSoA(s.soa, s.start+i, s.start+j)
}

func (s bucketSoASorter) Less(i, j int) bool {
	ia := s.start + i
	ib := s.start + j
	sa := effectiveScoreWithBoost(s.soa.Bids[ia], s.soa.CTRPPM[ia], s.soa.BoostPPM[ia])
	sb := effectiveScoreWithBoost(s.soa.Bids[ib], s.soa.CTRPPM[ib], s.soa.BoostPPM[ib])
	if sa != sb {
		return sa > sb
	}
	if s.soa.Weights[ia] != s.soa.Weights[ib] {
		return s.soa.Weights[ia] > s.soa.Weights[ib]
	}
	return s.soa.Bids[ia] > s.soa.Bids[ib]
}

func sortBucketSoA(soa *candidateBucketSoA, start, end int) {
	if soa == nil || end-start < 2 {
		return
	}
	sort.Sort(bucketSoASorter{soa: soa, start: start, end: end})
}

func sortRegistryBuckets(reg *CampaignAuctionRegistry) {
	if reg == nil || reg.GeoBucketCount == 0 {
		return
	}
	for i := 0; i < reg.GeoBucketCount; i++ {
		start := int(reg.GeoBucketStart[i])
		end := int(reg.GeoBucketStart[i+1])
		sortBucketSoA(&reg.GeoBucketSoA, start, end)
	}
	if reg.TargetBucketCount == 0 {
		return
	}
	for i := 0; i < reg.TargetBucketCount; i++ {
		start := int(reg.TargetBucketStart[i])
		end := int(reg.TargetBucketStart[i+1])
		sortBucketSoA(&reg.TargetBucketSoA, start, end)
	}
}

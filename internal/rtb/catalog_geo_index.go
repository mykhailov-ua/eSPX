package rtb

import (
	"sort"
)

// geoRange returns the half-open [start,end) slice into GeoBucketSoA for one geo hash.
func (reg *CampaignAuctionRegistry) geoRange(geoHash uint32) (start int, end int, ok bool) {
	if reg == nil || reg.GeoBucketCount == 0 {
		return 0, 0, false
	}
	hashes := reg.GeoBucketHash
	idx := sort.Search(reg.GeoBucketCount, func(i int) bool {
		return hashes[i] >= geoHash
	})
	if idx >= reg.GeoBucketCount || hashes[idx] != geoHash {
		return 0, 0, false
	}
	start = int(reg.GeoBucketStart[idx])
	end = int(reg.GeoBucketStart[idx+1])
	return start, end, true
}

// buildGeoIndex materializes per-geo candidate SoA buckets on the cold catalog rebuild path.
func buildGeoIndex(reg *CampaignAuctionRegistry) {
	if reg == nil || reg.Count == 0 {
		reg.GeoBucketCount = 0
		resetBucketSoA(&reg.GeoBucketSoA)
		return
	}

	buckets := make(map[uint32][]uint32, reg.Count)
	for i := 0; i < reg.Count; i++ {
		geo := reg.GeoHashes[i]
		buckets[geo] = append(buckets[geo], uint32(i))
	}

	reg.GeoBucketCount = len(buckets)
	reg.GeoBucketHash = make([]uint32, 0, reg.GeoBucketCount)
	for geo := range buckets {
		reg.GeoBucketHash = append(reg.GeoBucketHash, geo)
	}
	sort.Slice(reg.GeoBucketHash, func(i, j int) bool {
		return reg.GeoBucketHash[i] < reg.GeoBucketHash[j]
	})

	reg.GeoBucketStart = make([]uint32, reg.GeoBucketCount+1)
	resetBucketSoA(&reg.GeoBucketSoA)
	ensureBucketSoACap(&reg.GeoBucketSoA, reg.Count)
	for i, geo := range reg.GeoBucketHash {
		reg.GeoBucketStart[i] = uint32(reg.GeoBucketSoA.len())
		for _, catalogIdx := range buckets[geo] {
			appendBucketCandidate(&reg.GeoBucketSoA, reg, catalogIdx)
		}
	}
	reg.GeoBucketStart[reg.GeoBucketCount] = uint32(reg.GeoBucketSoA.len())
}

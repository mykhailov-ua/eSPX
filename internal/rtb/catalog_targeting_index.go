package rtb

import (
	"math/bits"
	"sort"
)

// Targeting index fields are populated on the cold catalog rebuild path when enabled.
// Hot path uses targetingRange for O(log n) lookup into a narrow candidate bucket.

func targetingBucketKey(geo uint32, deviceBit uint8, categoryBit uint64) uint64 {
	devIdx := uint64(bits.TrailingZeros8(deviceBit))
	catIdx := uint64(bits.TrailingZeros64(categoryBit))
	return (uint64(geo) << 32) | (devIdx << 16) | catIdx
}

func forEachDeviceBit(mask uint8, fn func(bit uint8)) {
	for bit := uint8(1); bit != 0; bit <<= 1 {
		if mask&bit != 0 {
			fn(bit)
		}
	}
}

func forEachCategoryBit(mask uint64, fn func(bit uint64)) {
	if mask == 0 {
		fn(1)
		return
	}
	for bit := uint64(1); bit != 0; bit <<= 1 {
		if mask&bit != 0 {
			fn(bit)
		}
	}
}

// buildTargetingIndex materializes geo+device+category inverted lists on the cold rebuild path.
func buildTargetingIndex(reg *CampaignAuctionRegistry) {
	if reg == nil || reg.Count == 0 {
		reg.TargetBucketCount = 0
		return
	}

	buckets := make(map[uint64][]uint32, reg.Count)
	for i := 0; i < reg.Count; i++ {
		geo := reg.GeoHashes[i]
		forEachDeviceBit(reg.DeviceMasks[i], func(deviceBit uint8) {
			forEachCategoryBit(reg.CategoryMasks[i], func(categoryBit uint64) {
				key := targetingBucketKey(geo, deviceBit, categoryBit)
				buckets[key] = append(buckets[key], uint32(i))
			})
		})
	}

	reg.TargetBucketCount = len(buckets)
	reg.TargetBucketKey = make([]uint64, 0, reg.TargetBucketCount)
	for key := range buckets {
		reg.TargetBucketKey = append(reg.TargetBucketKey, key)
	}
	sort.Slice(reg.TargetBucketKey, func(i, j int) bool {
		return reg.TargetBucketKey[i] < reg.TargetBucketKey[j]
	})

	reg.TargetBucketStart = make([]uint32, reg.TargetBucketCount+1)
	reg.TargetBucketIdx = make([]uint32, 0, reg.Count)
	for i, key := range reg.TargetBucketKey {
		reg.TargetBucketStart[i] = uint32(len(reg.TargetBucketIdx))
		reg.TargetBucketIdx = append(reg.TargetBucketIdx, buckets[key]...)
	}
	reg.TargetBucketStart[reg.TargetBucketCount] = uint32(len(reg.TargetBucketIdx))
}

// targetingRange returns the half-open [start,end) slice into TargetBucketIdx for one request tuple.
func (reg *CampaignAuctionRegistry) targetingRange(geo uint32, deviceType uint8, categoryMask uint64) (start int, end int, ok bool) {
	if reg == nil || reg.TargetBucketCount == 0 {
		return 0, 0, false
	}
	if deviceType == 0 {
		deviceType = 1
	}
	if categoryMask == 0 {
		categoryMask = 1
	}
	key := targetingBucketKey(geo, deviceType, categoryMask)
	keys := reg.TargetBucketKey
	idx := sort.Search(reg.TargetBucketCount, func(i int) bool {
		return keys[i] >= key
	})
	if idx >= reg.TargetBucketCount || keys[idx] != key {
		return 0, 0, false
	}
	start = int(reg.TargetBucketStart[idx])
	end = int(reg.TargetBucketStart[idx+1])
	return start, end, true
}

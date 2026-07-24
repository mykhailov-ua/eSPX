//go:build jumphash

package ingestion

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// randSeedSeq mixes per-goroutine RNG seeds to reduce collision under concurrency.
var randSeedSeq atomic.Int64

// randPool recycles math/rand sources for alias sampling on the jumphash canary path.
var randPool = sync.Pool{
	New: func() any {
		seed := time.Now().UnixNano() ^ randSeedSeq.Add(1)
		return rand.New(rand.NewSource(seed))
	},
}

// SelectAndShard picks a campaign and spreads hot traffic across sub-shards by user.
// Restricted to jumphash build tag — must not ship on the StaticSlot production tracker (M9-07).
func (hb *HybridBalancer) SelectAndShard(userID string, currentCampaignRps int64) (*CampaignMeta, int) {
	table := hb.aliasTable.Load()
	if table == nil || len(table.prob) == 0 {
		return nil, 0
	}

	n := len(table.prob)
	r := randPool.Get().(*rand.Rand)
	idx := r.Intn(n)

	selectedIdx := idx
	if r.Float64() >= table.prob[idx] {
		selectedIdx = table.alias[idx]
	}
	randPool.Put(r)

	campaign := table.campaigns[selectedIdx]
	if hb.totalShards <= 0 {
		return campaign, 0
	}

	isHot := hb.maxRpsPerNode > 0 && currentCampaignRps > hb.maxRpsPerNode
	var shard int

	if !isHot {
		shard = int(jumpHash(uint64(crc32Castagnoli(&campaign.ID)), int32(hb.totalShards)))
	} else {
		subShardCount := int(currentCampaignRps/hb.maxRpsPerNode) + 1
		if subShardCount > hb.totalShards {
			subShardCount = hb.totalShards
		}
		if subShardCount <= 0 {
			subShardCount = 1
		}

		hasher := fnv.New32a()
		_, _ = hasher.Write([]byte(userID))
		userHash := hasher.Sum32()
		subShardIdx := userHash % uint32(subShardCount)

		combinedHash := uint64(crc32Castagnoli(&campaign.ID)) ^ uint64(subShardIdx)
		shard = int(jumpHash(combinedHash, int32(hb.totalShards)))
	}

	return campaign, shard
}

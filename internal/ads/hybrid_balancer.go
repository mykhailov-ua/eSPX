// Package ads implements HybridBalancer, a campaign selector that combines
// weighted random sampling (Vose's alias method, O(1) per sample) with
// consistent jump-hash sharding for Redis key routing.
//
// Weight calculation per campaign:
//
//	w = BidMicro * CTR * sqrt(budgetRatio) * pacingFactor
//
// where pacingFactor is a sinusoidal approximation of the intended spend curve
// scaled by PeakTrafficFactor. Weights are re-normalised to sum to n before
// building the alias table.
//
// For hot campaigns (currentCampaignRps > maxRpsPerNode), the shard is derived
// by XOR-combining the campaign's base jump-hash with a user-keyed FNV32a
// sub-shard index. This distributes bursts across multiple Redis keys without
// changing the total shard count, preventing hot-key saturation on a single node.
package ads

import (
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// CampaignMeta carries the scoring inputs required by UpdateCampaigns. The fields
// are read-only during sampling; they must not be mutated after being passed to
// UpdateCampaigns because the pointer is stored inside the alias table snapshot.
type CampaignMeta struct {
	ID                uuid.UUID
	BidMicro          int64
	CTR               float64
	RemainingBudget   int64
	TotalBudget       int64
	PeakTrafficFactor float64
}

type voseAliasTable struct {
	campaigns []*CampaignMeta
	prob      []float64
	alias     []int
}

// HybridBalancer stores an atomic pointer to a voseAliasTable snapshot. UpdateCampaigns
// replaces the pointer in one atomic store; SelectAndShard loads it without locking.
// randPool provides per-call PRNG instances to avoid sharing state across goroutines.
type HybridBalancer struct {
	totalShards   int
	maxRpsPerNode int64
	aliasTable    atomic.Pointer[voseAliasTable]
}

var (
	randSeedSeq atomic.Int64
	randPool    = sync.Pool{
		New: func() any {

			seed := time.Now().UnixNano() ^ randSeedSeq.Add(1)
			return rand.New(rand.NewSource(seed))
		},
	}
)

func NewHybridBalancer(totalShards int, maxRpsPerNode int) *HybridBalancer {
	return &HybridBalancer{
		totalShards:   totalShards,
		maxRpsPerNode: int64(maxRpsPerNode),
	}
}

// UpdateCampaigns builds a new Vose alias table from campaigns and atomically
// replaces the current snapshot. secondsElapsed and totalSeconds parameterise
// the pacing sinusoid; if totalSeconds is 0 the pacing factor degenerates to 0.
// Invalid (nil) campaign pointers are silently filtered before weight computation.
func (hb *HybridBalancer) UpdateCampaigns(campaigns []*CampaignMeta, secondsElapsed int64, totalSeconds int64) {

	validCampaigns := make([]*CampaignMeta, 0, len(campaigns))
	for _, c := range campaigns {
		if c != nil {
			validCampaigns = append(validCampaigns, c)
		}
	}
	n := len(validCampaigns)
	if n == 0 {
		hb.aliasTable.Store(nil)
		return
	}

	weights := make([]float64, n)
	sum := 0.0

	for i, c := range validCampaigns {
		var linearRatio float64
		if totalSeconds > 0 {
			linearRatio = float64(secondsElapsed) / float64(totalSeconds)
		}
		pacingFactor := linearRatio + (c.PeakTrafficFactor * math.Sin(linearRatio*math.Pi))

		var budgetRatio float64
		if c.TotalBudget > 0 {
			budgetRatio = float64(c.RemainingBudget) / float64(c.TotalBudget)
		}
		if budgetRatio < 0.0 {
			budgetRatio = 0.0
		}

		w := float64(c.BidMicro) * c.CTR * math.Sqrt(budgetRatio) * pacingFactor
		if w < 0.0 || math.IsNaN(w) || math.IsInf(w, 0) {
			w = 0.0
		}
		weights[i] = w
		sum += w
	}

	if sum <= 0 || math.IsNaN(sum) || math.IsInf(sum, 0) {
		hb.aliasTable.Store(nil)
		return
	}

	normWeights := make([]float64, n)
	for i, w := range weights {
		normWeights[i] = w * float64(n) / sum
	}

	small := make([]int, 0, n)
	large := make([]int, 0, n)
	for i, w := range normWeights {
		if w < 1.0 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}

	prob := make([]float64, n)
	alias := make([]int, n)

	for len(small) > 0 && len(large) > 0 {
		s := small[len(small)-1]
		small = small[:len(small)-1]

		l := large[len(large)-1]
		large = large[:len(large)-1]

		prob[s] = normWeights[s]
		alias[s] = l

		normWeights[l] = (normWeights[l] + normWeights[s]) - 1.0
		if normWeights[l] < 1.0 {
			small = append(small, l)
		} else {
			large = append(large, l)
		}
	}

	for len(large) > 0 {
		l := large[len(large)-1]
		large = large[:len(large)-1]
		prob[l] = 1.0
	}
	for len(small) > 0 {
		s := small[len(small)-1]
		small = small[:len(small)-1]
		prob[s] = 1.0
	}

	hb.aliasTable.Store(&voseAliasTable{
		campaigns: validCampaigns,
		prob:      prob,
		alias:     alias,
	})
}

// SelectAndShard samples a campaign from the alias table and maps it to a Redis shard.
// For normal load the shard is the deterministic jump-hash of the campaign ID.
// For hot load (currentCampaignRps > maxRpsPerNode), a FNV32a hash of userID selects
// a sub-shard; the campaign's jump-hash and the sub-shard index are XOR-combined
// before the final jump-hash call to spread the load across shards.
// Returns (nil, 0) if no campaigns are registered.
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
		shard = int(jumpHash(uint64(crc32IEEE(campaign.ID)), int32(hb.totalShards)))
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

		combinedHash := uint64(crc32IEEE(campaign.ID)) ^ uint64(subShardIdx)
		shard = int(jumpHash(combinedHash, int32(hb.totalShards)))
	}

	return campaign, shard
}

package ingestion

import (
	"math"
	"sync/atomic"

	"github.com/google/uuid"
)

// CampaignMeta carries auction inputs for weighted campaign selection and sharding.
type CampaignMeta struct {
	ID                uuid.UUID
	BidMicro          int64
	CTR               float64
	RemainingBudget   int64
	TotalBudget       int64
	PeakTrafficFactor float64
}

// voseAliasTable enables O(1) weighted random campaign selection after an offline rebuild.
type voseAliasTable struct {
	campaigns []*CampaignMeta
	prob      []float64
	alias     []int
	weights   map[uuid.UUID]uint32
}

// HybridBalancer selects campaigns and Redis shards for RTB traffic spreading.
type HybridBalancer struct {
	totalShards   int
	maxRpsPerNode int64
	aliasTable    atomic.Pointer[voseAliasTable]
	weightSnap    atomic.Pointer[map[uuid.UUID]uint32]
}

func NewHybridBalancer(totalShards int, maxRpsPerNode int) *HybridBalancer {
	return &HybridBalancer{
		totalShards:   totalShards,
		maxRpsPerNode: int64(maxRpsPerNode),
	}
}

// UpdateCampaigns rebuilds the alias table from current campaign weights and pacing state.
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

	weightMap := buildHybridWeightMap(validCampaigns, weights)
	hb.aliasTable.Store(&voseAliasTable{
		campaigns: validCampaigns,
		prob:      prob,
		alias:     alias,
		weights:   weightMap,
	})
	hb.weightSnap.Store(&weightMap)
}

func buildHybridWeightMap(campaigns []*CampaignMeta, raw []float64) map[uuid.UUID]uint32 {
	out := make(map[uuid.UUID]uint32, len(campaigns))
	for i, c := range campaigns {
		if c == nil {
			continue
		}
		w := raw[i]
		if w <= 0 || math.IsNaN(w) || math.IsInf(w, 0) {
			out[c.ID] = 1
			continue
		}
		scaled := w * 1000
		if scaled > float64(math.MaxUint32) {
			out[c.ID] = math.MaxUint32
		} else if scaled < 1 {
			out[c.ID] = 1
		} else {
			out[c.ID] = uint32(scaled)
		}
	}
	return out
}

// WeightFor returns hybrid ranking weight for a campaign (R11); minimum 1.
func (hb *HybridBalancer) WeightFor(id uuid.UUID) uint32 {
	if hb == nil {
		return 1
	}
	ptr := hb.weightSnap.Load()
	if ptr == nil || *ptr == nil {
		return 1
	}
	w, ok := (*ptr)[id]
	if !ok || w == 0 {
		return 1
	}
	return w
}

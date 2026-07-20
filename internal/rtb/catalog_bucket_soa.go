package rtb

// candidateBucketSoA stores hot-path candidate fields in bucket iteration order.
// All slices share the same length; populated on the cold catalog rebuild path.
type candidateBucketSoA struct {
	CatalogIdx            []uint32
	Bids                  []int64
	CTRPPM                []uint32
	Reserves              []int64
	DailyBudgets          []int64
	PacingOpen            []uint8
	DeviceMasks           []uint8
	CategoryMasks         []uint64
	Weights               []uint32
	BudgetIndices         []uint32
	CustomerBudgetIndices []uint32
}

func (soa *candidateBucketSoA) len() int {
	if soa == nil {
		return 0
	}
	return len(soa.CatalogIdx)
}

func (soa *candidateBucketSoA) slicesValid(end int) bool {
	if soa == nil || end < 0 || end > len(soa.CatalogIdx) {
		return false
	}
	return end <= len(soa.Bids) &&
		end <= len(soa.CTRPPM) &&
		end <= len(soa.Reserves) &&
		end <= len(soa.DailyBudgets) &&
		end <= len(soa.PacingOpen) &&
		end <= len(soa.DeviceMasks) &&
		end <= len(soa.CategoryMasks) &&
		end <= len(soa.Weights) &&
		end <= len(soa.BudgetIndices) &&
		end <= len(soa.CustomerBudgetIndices)
}

func resetBucketSoA(soa *candidateBucketSoA) {
	if soa == nil {
		return
	}
	soa.CatalogIdx = soa.CatalogIdx[:0]
	soa.Bids = soa.Bids[:0]
	soa.CTRPPM = soa.CTRPPM[:0]
	soa.Reserves = soa.Reserves[:0]
	soa.DailyBudgets = soa.DailyBudgets[:0]
	soa.PacingOpen = soa.PacingOpen[:0]
	soa.DeviceMasks = soa.DeviceMasks[:0]
	soa.CategoryMasks = soa.CategoryMasks[:0]
	soa.Weights = soa.Weights[:0]
	soa.BudgetIndices = soa.BudgetIndices[:0]
	soa.CustomerBudgetIndices = soa.CustomerBudgetIndices[:0]
}

func appendBucketCandidate(soa *candidateBucketSoA, reg *CampaignAuctionRegistry, catalogIdx uint32) {
	i := int(catalogIdx)
	soa.CatalogIdx = append(soa.CatalogIdx, catalogIdx)
	soa.Bids = append(soa.Bids, reg.Bids[i])
	soa.CTRPPM = append(soa.CTRPPM, reg.CTRPPM[i])
	soa.Reserves = append(soa.Reserves, reg.Reserves[i])
	soa.DailyBudgets = append(soa.DailyBudgets, reg.DailyBudgets[i])
	soa.PacingOpen = append(soa.PacingOpen, reg.PacingOpen[i])
	soa.DeviceMasks = append(soa.DeviceMasks, reg.DeviceMasks[i])
	soa.CategoryMasks = append(soa.CategoryMasks, reg.CategoryMasks[i])
	soa.Weights = append(soa.Weights, reg.Weights[i])
	soa.BudgetIndices = append(soa.BudgetIndices, reg.BudgetIndices[i])
	soa.CustomerBudgetIndices = append(soa.CustomerBudgetIndices, reg.CustomerBudgetIndices[i])
}

func ensureBucketSoACap(soa *candidateBucketSoA, wantCap int) {
	if wantCap <= 0 {
		return
	}
	if cap(soa.CatalogIdx) < wantCap {
		soa.CatalogIdx = make([]uint32, 0, wantCap)
		soa.Bids = make([]int64, 0, wantCap)
		soa.CTRPPM = make([]uint32, 0, wantCap)
		soa.Reserves = make([]int64, 0, wantCap)
		soa.DailyBudgets = make([]int64, 0, wantCap)
		soa.PacingOpen = make([]uint8, 0, wantCap)
		soa.DeviceMasks = make([]uint8, 0, wantCap)
		soa.CategoryMasks = make([]uint64, 0, wantCap)
		soa.Weights = make([]uint32, 0, wantCap)
		soa.BudgetIndices = make([]uint32, 0, wantCap)
		soa.CustomerBudgetIndices = make([]uint32, 0, wantCap)
	}
}

func swapBucketSoA(soa *candidateBucketSoA, i, j int) {
	soa.CatalogIdx[i], soa.CatalogIdx[j] = soa.CatalogIdx[j], soa.CatalogIdx[i]
	soa.Bids[i], soa.Bids[j] = soa.Bids[j], soa.Bids[i]
	soa.CTRPPM[i], soa.CTRPPM[j] = soa.CTRPPM[j], soa.CTRPPM[i]
	soa.Reserves[i], soa.Reserves[j] = soa.Reserves[j], soa.Reserves[i]
	soa.DailyBudgets[i], soa.DailyBudgets[j] = soa.DailyBudgets[j], soa.DailyBudgets[i]
	soa.PacingOpen[i], soa.PacingOpen[j] = soa.PacingOpen[j], soa.PacingOpen[i]
	soa.DeviceMasks[i], soa.DeviceMasks[j] = soa.DeviceMasks[j], soa.DeviceMasks[i]
	soa.CategoryMasks[i], soa.CategoryMasks[j] = soa.CategoryMasks[j], soa.CategoryMasks[i]
	soa.Weights[i], soa.Weights[j] = soa.Weights[j], soa.Weights[i]
	soa.BudgetIndices[i], soa.BudgetIndices[j] = soa.BudgetIndices[j], soa.BudgetIndices[i]
	soa.CustomerBudgetIndices[i], soa.CustomerBudgetIndices[j] = soa.CustomerBudgetIndices[j], soa.CustomerBudgetIndices[i]
}

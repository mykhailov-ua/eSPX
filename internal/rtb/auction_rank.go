package rtb

const (
	rankDeadlineCheckMask = 31
	rankMaxScanCandidates = 500
)

// GeoBitFromHash maps a geo hash to a single targeting bitmask bit.
func GeoBitFromHash(geoHash uint32) uint64 {
	return uint64(1) << (geoHash & 63)
}

func (registry *Registry) catalogSlicesValid(reg *CampaignAuctionRegistry) bool {
	count := reg.Count
	if !(count <= len(reg.CampaignIDs) && count <= len(reg.Bids) &&
		count <= len(reg.CTRPPM) && count <= len(reg.Reserves) &&
		count <= len(reg.DailyBudgets) && count <= len(reg.PacingOpen) &&
		count <= len(reg.DeviceMasks) && count <= len(reg.CategoryMasks) &&
		count <= len(reg.GeoHashes) && count <= len(reg.Weights) &&
		count <= len(reg.BoostPPM) &&
		count <= len(reg.BudgetIndices) && count <= len(reg.CustomerBudgetIndices)) {
		return false
	}
	geoEnd := reg.GeoBucketSoA.len()
	if geoEnd > 0 && !reg.GeoBucketSoA.slicesValid(geoEnd) {
		return false
	}
	targetEnd := reg.TargetBucketSoA.len()
	if targetEnd > 0 && !reg.TargetBucketSoA.slicesValid(targetEnd) {
		return false
	}
	return true
}

func bidsAt(reg *CampaignAuctionRegistry, idx int) int64 {
	return reg.Bids[idx]
}

func (registry *Registry) candidateRange(
	reg *CampaignAuctionRegistry,
	req *BidRequest,
) (soa *candidateBucketSoA, start int, end int, ok bool) {
	if registry.targetingIndexEnabled.Load() {
		if start, end, ok = reg.targetingRange(req.GeoHash, req.DeviceType, req.CategoryMask); ok {
			return &reg.TargetBucketSoA, start, end, true
		}
	}
	start, end, ok = reg.geoRange(req.GeoHash)
	return &reg.GeoBucketSoA, start, end, ok
}

func (registry *Registry) rankCandidates(
	reg *CampaignAuctionRegistry,
	req *BidRequest,
	soa *candidateBucketSoA,
	bucketStart int,
	bucketEnd int,
) (winnerIdx int, secondBid int64, scanned int, noBid NoBidReason) {
	winnerIdx = -1
	var maxScore int64 = -1
	secondBid = -1
	var pacingBlocked bool
	var dailyBlocked bool

	if soa == nil || !soa.slicesValid(bucketEnd) {
		return -1, -1, 0, NoBidCorruptCatalog
	}
	// BCE: one window check eliminates per-iteration bounds checks on bucket slices.
	if bucketStart < 0 || bucketEnd < bucketStart || bucketEnd > soa.len() {
		return -1, -1, 0, NoBidCorruptCatalog
	}

	catalogIdx := soa.CatalogIdx[bucketStart:bucketEnd]
	bids := soa.Bids[bucketStart:bucketEnd]
	ctrppm := soa.CTRPPM[bucketStart:bucketEnd]
	reserves := soa.Reserves[bucketStart:bucketEnd]
	dailyBudgets := soa.DailyBudgets[bucketStart:bucketEnd]
	pacingOpen := soa.PacingOpen[bucketStart:bucketEnd]
	deviceMasks := soa.DeviceMasks[bucketStart:bucketEnd]
	categoryMasks := soa.CategoryMasks[bucketStart:bucketEnd]
	weights := soa.Weights[bucketStart:bucketEnd]
	boostPPM := soa.BoostPPM[bucketStart:bucketEnd]
	budgetIndices := soa.BudgetIndices[bucketStart:bucketEnd]
	customerBudgetIndices := soa.CustomerBudgetIndices[bucketStart:bucketEnd]

	regCount := reg.Count
	deviceType := req.DeviceType
	categoryMask := req.CategoryMask
	minBid := req.MinBid
	store := registry.store
	winnerPos := -1
	deadline := req.DeadlineMono
	hasDeadline := deadline > 0

	for pos := 0; pos < len(catalogIdx); pos++ {
		scanned++
		if scanned > rankMaxScanCandidates {
			return -1, -1, scanned, NoBidScanLimit
		}
		if hasDeadline && scanned&rankDeadlineCheckMask == 0 && monotonicNano() > deadline {
			return -1, -1, scanned, NoBidTimeout
		}
		i := int(catalogIdx[pos])
		if i < 0 || i >= regCount {
			return -1, -1, scanned, NoBidCorruptCatalog
		}

		if pacingOpen[pos] == PacingClosed {
			pacingBlocked = true
			continue
		}
		if (deviceMasks[pos] & deviceType) == 0 {
			continue
		}
		if (categoryMasks[pos] & categoryMask) == 0 {
			continue
		}

		bid := bids[pos]
		reserve := reserves[pos]
		if bid < reserve || bid < minBid {
			continue
		}

		budgetIdx := budgetIndices[pos]
		if !store.budgetSlotExists(budgetIdx) {
			return -1, -1, scanned, NoBidCorruptCatalog
		}
		if store.LoadBudget(budgetIdx) < bid {
			continue
		}
		if dailyBudgets[pos] > 0 && store.loadDailyHeadroom(budgetIdx, dailyBudgets[pos]) < bid {
			dailyBlocked = true
			continue
		}
		customerIdx := customerBudgetIndices[pos]
		if customerIdx != invalidCustomerBudgetIdx && store.LoadCustomerBudget(customerIdx) < bid {
			continue
		}

		score := effectiveScoreWithBoost(bid, ctrppm[pos], boostPPM[pos])
		if winnerIdx >= 0 && secondBid >= 0 && score < maxScore {
			break
		}
		if score > maxScore {
			if winnerIdx >= 0 {
				secondBid = bids[winnerPos]
			}
			maxScore = score
			winnerIdx = i
			winnerPos = pos
		} else if score == maxScore && winnerIdx >= 0 {
			if weights[pos] > weights[winnerPos] {
				secondBid = bids[winnerPos]
				winnerIdx = i
				winnerPos = pos
			}
			if bid > secondBid {
				secondBid = bid
			}
		} else if winnerIdx >= 0 && bid > secondBid {
			secondBid = bid
		}
	}

	if winnerIdx == -1 {
		if pacingBlocked {
			return -1, -1, scanned, NoBidPacingClosed
		}
		if dailyBlocked {
			return -1, -1, scanned, NoBidDailyCapExceeded
		}
		return -1, -1, scanned, NoBidNoCandidates
	}
	return winnerIdx, secondBid, scanned, NoBidNone
}

package rtb

func (registry *Registry) catalogSlicesValid(reg *CampaignAuctionRegistry) bool {
	count := reg.Count
	return count <= len(reg.CampaignIDs) && count <= len(reg.Bids) &&
		count <= len(reg.CTRPPM) && count <= len(reg.Reserves) &&
		count <= len(reg.DailyBudgets) && count <= len(reg.PacingOpen) &&
		count <= len(reg.DeviceMasks) && count <= len(reg.CategoryMasks) &&
		count <= len(reg.GeoHashes) && count <= len(reg.Weights) &&
		count <= len(reg.BudgetIndices) && count <= len(reg.CustomerBudgetIndices)
}

func bidsAt(reg *CampaignAuctionRegistry, idx int) int64 {
	return reg.Bids[idx]
}

func (registry *Registry) candidateRange(
	reg *CampaignAuctionRegistry,
	req *BidRequest,
) (bucket []uint32, start int, end int, ok bool) {
	if registry.targetingIndexEnabled.Load() {
		if start, end, ok = reg.targetingRange(req.GeoHash, req.DeviceType, req.CategoryMask); ok {
			return reg.TargetBucketIdx, start, end, true
		}
	}
	start, end, ok = reg.geoRange(req.GeoHash)
	return reg.GeoBucketIdx, start, end, ok
}

func (registry *Registry) rankCandidates(
	reg *CampaignAuctionRegistry,
	req *BidRequest,
	bucket []uint32,
	bucketStart int,
	bucketEnd int,
) (winnerIdx int, secondBid int64, scanned int, noBid NoBidReason) {
	winnerIdx = -1
	var maxScore int64 = -1
	secondBid = -1
	var pacingBlocked bool
	var dailyBlocked bool

	for pos := bucketStart; pos < bucketEnd; pos++ {
		scanned++
		i := int(bucket[pos])
		if i < 0 || i >= reg.Count {
			return -1, -1, scanned, NoBidCorruptCatalog
		}

		if reg.PacingOpen[i] == PacingClosed {
			pacingBlocked = true
			continue
		}
		if (reg.DeviceMasks[i] & req.DeviceType) == 0 {
			continue
		}
		if (reg.CategoryMasks[i] & req.CategoryMask) == 0 {
			continue
		}

		bid := reg.Bids[i]
		reserve := reg.Reserves[i]
		if bid < reserve || bid < req.MinBid {
			continue
		}

		budgetIdx := reg.BudgetIndices[i]
		if !registry.store.budgetSlotExists(budgetIdx) {
			return -1, -1, scanned, NoBidCorruptCatalog
		}
		if registry.store.LoadBudget(budgetIdx) < bid {
			continue
		}
		if reg.DailyBudgets[i] > 0 && registry.store.loadDailyHeadroom(budgetIdx, reg.DailyBudgets[i]) < bid {
			dailyBlocked = true
			continue
		}
		customerIdx := reg.CustomerBudgetIndices[i]
		if customerIdx != invalidCustomerBudgetIdx && registry.store.LoadCustomerBudget(customerIdx) < bid {
			continue
		}

		score := effectiveScore(bid, reg.CTRPPM[i])
		if score > maxScore {
			if winnerIdx >= 0 {
				secondBid = reg.Bids[winnerIdx]
			}
			maxScore = score
			winnerIdx = i
		} else if score == maxScore && winnerIdx >= 0 {
			if reg.Weights[i] > reg.Weights[winnerIdx] {
				secondBid = reg.Bids[winnerIdx]
				winnerIdx = i
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

package rtb

// RunAuction selects a campaign and debits the winner budget on the hot bid path.
func (registry *Registry) RunAuction(req *BidRequest) (AuctionResult, NoBidReason) {
	return registry.runAuction(req, true)
}

// RunAuctionEval runs the auction without debiting budget for shadow validation.
func (registry *Registry) RunAuctionEval(req *BidRequest) (AuctionResult, NoBidReason) {
	return registry.runAuction(req, false)
}

func (registry *Registry) runAuction(req *BidRequest, spend bool) (AuctionResult, NoBidReason) {
	start := auctionStartMono()

	if req == nil || req.MinBid < 0 {
		recordAuctionOutcome(start, NoBidInvalidRequest, 0)
		return AuctionResult{}, NoBidInvalidRequest
	}
	reg := registry.LoadShard(req.GeoHash)
	if reg == nil || reg.Count == 0 {
		recordAuctionOutcome(start, NoBidEmptyShard, 0)
		return AuctionResult{}, NoBidEmptyShard
	}

	if !registry.catalogSlicesValid(reg) {
		recordAuctionOutcome(start, NoBidCorruptCatalog, reg.Count)
		return AuctionResult{}, NoBidCorruptCatalog
	}

	bucket, bucketStart, bucketEnd, ok := registry.candidateRange(reg, req)
	if !ok {
		recordAuctionOutcome(start, NoBidNoCandidates, 0)
		return AuctionResult{}, NoBidNoCandidates
	}

	clearing := registry.ClearingMode()
	winnerIdx, secondBid, scanned, noBid := registry.rankCandidates(reg, req, bucket, bucketStart, bucketEnd)
	if noBid != NoBidNone {
		recordAuctionOutcome(start, noBid, scanned)
		return AuctionResult{}, noBid
	}

	price := registry.clearingPrice(clearing, req.MinBid, bidsAt(reg, winnerIdx), secondBid)
	price = applyReserve(price, reg.Reserves[winnerIdx], bidsAt(reg, winnerIdx))

	if winnerIdx >= len(reg.BudgetIndices) || winnerIdx >= len(reg.CampaignIDs) {
		recordAuctionOutcome(start, NoBidCorruptCatalog, scanned)
		return AuctionResult{}, NoBidCorruptCatalog
	}

	if spend {
		winnerBudgetIdx := reg.BudgetIndices[winnerIdx]
		customerIdx := reg.CustomerBudgetIndices[winnerIdx]
		dailyLimit := reg.DailyBudgets[winnerIdx]
		if !registry.store.CheckAndSpendAll(winnerBudgetIdx, customerIdx, price, dailyLimit) {
			recordAuctionOutcome(start, NoBidSpendFailed, scanned)
			return AuctionResult{}, NoBidSpendFailed
		}
	}

	recordAuctionOutcome(start, NoBidNone, scanned)
	return AuctionResult{
		CampaignID: reg.CampaignIDs[winnerIdx],
		Price:      price,
	}, NoBidNone
}

package rtb

// BidRequest carries targeting fields for a single bid in cache-friendly field order.
type BidRequest struct {
	CategoryMask uint64
	MinBid       int64
	GeoHash      uint32
	DeviceType   uint8
}

// AuctionResult carries the clearing outcome without heap allocation on the hot path.
type AuctionResult struct {
	CampaignID CampaignID
	Price      int64
}

// RunAuction selects a campaign on the hot bid path without locks, using second-price clearing
// so winners pay above the floor only when competition requires it.
//
// The shard pointer is pinned for the whole call (RCU). If UpdateCampaigns removes a campaign
// while an auction is in flight, the request may still clear spend for that campaign ID.
// This is intentional eventual consistency: catalog visibility lags one auction latency.
func (registry *Registry) RunAuction(req *BidRequest) (AuctionResult, bool) {
	if req == nil || req.MinBid < 0 {
		return AuctionResult{}, false
	}
	reg := registry.LoadShard(req.GeoHash)
	if reg == nil || reg.Count == 0 {
		return AuctionResult{}, false
	}

	count := reg.Count
	campaignIDs := reg.CampaignIDs
	bidFloors := reg.BidFloors
	deviceMasks := reg.DeviceMasks
	categoryMasks := reg.CategoryMasks
	geoHashes := reg.GeoHashes
	budgetIndices := reg.BudgetIndices

	if count > len(campaignIDs) || count > len(bidFloors) || count > len(deviceMasks) ||
		count > len(categoryMasks) || count > len(geoHashes) || count > len(budgetIndices) {
		return AuctionResult{}, false
	}

	var winnerIdx int = -1
	var maxBid int64 = -1
	var secondBid int64 = -1

	for i := 0; i < count; i++ {
		if geoHashes[i] != req.GeoHash {
			continue
		}
		if (deviceMasks[i] & req.DeviceType) == 0 {
			continue
		}
		if (categoryMasks[i] & req.CategoryMask) == 0 {
			continue
		}
		bid := bidFloors[i]
		if bid < req.MinBid {
			continue
		}
		budgetIdx := budgetIndices[i]
		if registry.store.LoadBudget(budgetIdx) < bid {
			continue
		}

		if bid > maxBid {
			secondBid = maxBid
			maxBid = bid
			winnerIdx = i
		} else if bid > secondBid {
			secondBid = bid
		}
	}

	if winnerIdx == -1 {
		return AuctionResult{}, false
	}

	price := req.MinBid
	if secondBid != -1 && secondBid > price {
		price = secondBid
	}

	if winnerIdx >= len(budgetIndices) || winnerIdx >= len(campaignIDs) {
		return AuctionResult{}, false
	}

	winnerBudgetIdx := budgetIndices[winnerIdx]
	if registry.store.LoadBudget(winnerBudgetIdx) < price {
		return AuctionResult{}, false
	}
	if !registry.store.CheckAndSpend(winnerBudgetIdx, price) {
		return AuctionResult{}, false
	}

	return AuctionResult{
		CampaignID: campaignIDs[winnerIdx],
		Price:      price,
	}, true
}

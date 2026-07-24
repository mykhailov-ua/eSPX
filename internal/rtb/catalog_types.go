package rtb

// CampaignID is a fixed-width campaign identifier used on the bid hot path.
type CampaignID uint64

// CustomerID is a fixed-width advertiser key for shared customer budget pools.
type CustomerID uint64

// BidRequest carries targeting fields for a single bid in cache-friendly field order.
type BidRequest struct {
	CategoryMask uint64
	MinBid       int64
	GeoHash      uint32
	DeviceType   uint8
	DeadlineMono int64
	DealBlock    NoBidReason // pre-validated PMP gate; NoBidNone when absent or passing
}

// AuctionResult carries the clearing outcome without heap allocation on the hot path.
type AuctionResult struct {
	CampaignID CampaignID
	Price      int64
}

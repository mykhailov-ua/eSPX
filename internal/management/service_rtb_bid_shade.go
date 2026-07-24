package management

import (
	"context"

	"espx/internal/config"
	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/rtb"
)

// RtbBidShadeRequest is the admin bid-shading simulation input (R16).
type RtbBidShadeRequest struct {
	GeoHash      uint32 `json:"geo_hash"`
	DeviceType   uint8  `json:"device_type"`
	CategoryMask uint64 `json:"category_mask"`
	MinBidMicro  int64  `json:"min_bid_micro"`
}

// RtbBidShadeResponse carries shading recommendation from shadow auction eval.
type RtbBidShadeResponse struct {
	HasBid              bool    `json:"has_bid"`
	CampaignID          string  `json:"campaign_id,omitempty"`
	ClearingPriceMicro  int64   `json:"clearing_price_micro"`
	RecommendedBidMicro int64   `json:"recommended_bid_micro"`
	ShadeDeltaMicro     int64   `json:"shade_delta_micro"`
	NoBidReason         string  `json:"no_bid_reason,omitempty"`
	SecondPriceDeltaPct float64 `json:"second_price_delta_pct"`
}

// SimulateRtbBidShade runs RunAuctionEval and recommends a shaded bid from second-price delta (R16).
func (s *Service) SimulateRtbBidShade(ctx context.Context, req RtbBidShadeRequest) (RtbBidShadeResponse, error) {
	out := RtbBidShadeResponse{}
	if s == nil || s.pool == nil {
		out.NoBidReason = rtb.NoBidInvalidRequest.String()
		return out, nil
	}
	registry := ingestion.NewRegistry(db.New(s.pool))
	if _, err := registry.Sync(ctx); err != nil {
		return out, err
	}
	cfg := s.cfg
	if cfg == nil {
		cfg = &config.Config{ClickAmount: 1, ImpressionAmount: 1}
	}
	catalog := ingestion.NewRtbCatalog(rtb.NewBudgetStore(), ingestion.BudgetAuthorityShadow)
	catalog.Registry().SetTargetingIndexEnabled(cfg.RtbTargetingIndexEnabled())
	ingestion.SyncRtbCatalog(ctx, registry, catalog, cfg, nil, ingestion.RtbBudgetSync{}, nil)

	targeting := ingestion.RtbTargetingInput{
		GeoHash:             req.GeoHash,
		DeviceType:          req.DeviceType,
		CategoryMask:        req.CategoryMask,
		PublisherFloorMicro: req.MinBidMicro,
	}
	if targeting.CategoryMask == 0 {
		targeting.CategoryMask = 1
	}
	bidReq := ingestion.BidRequestFromEvent(nil, targeting)
	res, reason := catalog.Registry().RunAuctionEval(&bidReq)
	if !reason.OK() {
		out.NoBidReason = reason.String()
		return out, nil
	}
	uid, ok := catalog.UUIDForWinner(res.CampaignID)
	if !ok {
		out.NoBidReason = rtb.NoBidNoCandidates.String()
		return out, nil
	}
	out.HasBid = true
	out.CampaignID = uid.String()
	out.ClearingPriceMicro = res.Price
	out.RecommendedBidMicro = res.Price - res.Price/50
	if out.RecommendedBidMicro < req.MinBidMicro {
		out.RecommendedBidMicro = req.MinBidMicro
	}
	out.ShadeDeltaMicro = res.Price - out.RecommendedBidMicro
	if res.Price > 0 {
		out.SecondPriceDeltaPct = float64(out.ShadeDeltaMicro) * 100 / float64(res.Price)
	}
	return out, nil
}

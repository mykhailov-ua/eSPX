package management

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
)

// DealWinLossRate holds ClickHouse win/loss counts for one PMP deal.
type DealWinLossRate struct {
	DealID   string
	Wins     uint64
	Losses   uint64
	WinRate  float64
	SampleN  uint64
}

// BidFloorRecommendationDTO is the optimizer output for one deal.
type BidFloorRecommendationDTO struct {
	DealID           string `json:"deal_id"`
	BaseFloorMicro   int64  `json:"base_floor_micro"`
	RecommendedMicro int64  `json:"recommended_floor_micro"`
	WinRate          float64 `json:"win_rate"`
	SampleN          uint64  `json:"sample_n"`
}

// computeRecommendedFloor adjusts a deal floor based on observed win rate.
func computeRecommendedFloor(base int64, rate float64, sampleN uint64, cfg *config.Config) int64 {
	if base < 0 {
		base = 0
	}
	if cfg == nil || sampleN == 0 {
		return base
	}
	minFloor := cfg.BidFloorMinMicro
	adjust := int64(cfg.BidFloorAdjustPct)

	out := base
	switch {
	case rate < cfg.BidFloorWinRateLow && base > 0:
		out = base - (base * adjust / 100)
	case rate > cfg.BidFloorWinRateHigh:
		out = base + (base * adjust / 100)
	}
	if out < minFloor {
		out = minFloor
	}
	return out
}

func (s *Service) queryClickHouseDealWinRates(ctx context.Context, lookbackHours int) (map[string]DealWinLossRate, error) {
	if s.ch == nil {
		return nil, nil
	}
	if lookbackHours < 1 {
		lookbackHours = 24
	}

	query := `
SELECT
    deal_id,
    countIf(outcome = 1) AS wins,
    countIf(outcome = 0) AS losses
FROM rtb_deal_outcomes
WHERE created_at >= now() - INTERVAL ? HOUR
GROUP BY deal_id`

	rows, err := s.ch.Query(ctx, query, lookbackHours)
	if err != nil {
		return nil, fmt.Errorf("clickhouse deal win rates: %w", err)
	}
	defer rows.Close()

	out := make(map[string]DealWinLossRate)
	for rows.Next() {
		var dealID string
		var wins, losses uint64
		if err := rows.Scan(&dealID, &wins, &losses); err != nil {
			return nil, err
		}
		total := wins + losses
		rate := 0.0
		if total > 0 {
			rate = float64(wins) / float64(total)
		}
		out[dealID] = DealWinLossRate{
			DealID:  dealID,
			Wins:    wins,
			Losses:  losses,
			WinRate: rate,
			SampleN: total,
		}
	}
	return out, rows.Err()
}

// OptimizeBidFloors queries ClickHouse win/loss rates and writes recommendations to Redis.
func (s *Service) OptimizeBidFloors(ctx context.Context) ([]BidFloorRecommendationDTO, error) {
	if len(s.rdbs) == 0 {
		return nil, fmt.Errorf("no redis client available")
	}

	rates, err := s.queryClickHouseDealWinRates(ctx, s.cfg.BidFloorLookbackHours)
	if err != nil {
		return nil, err
	}

	deals, err := db.New(s.GetPool()).ListRtbDeals(ctx)
	if err != nil {
		return nil, err
	}

	recs := make([]BidFloorRecommendationDTO, 0, len(deals))
	for _, deal := range deals {
		stats := rates[deal.DealID]
		rec := BidFloorRecommendationDTO{
			DealID:         deal.DealID,
			BaseFloorMicro: deal.FloorMicro,
			WinRate:        stats.WinRate,
			SampleN:        stats.SampleN,
		}
		rec.RecommendedMicro = computeRecommendedFloor(deal.FloorMicro, stats.WinRate, stats.SampleN, s.cfg)
		recs = append(recs, rec)

		val := strconv.FormatInt(rec.RecommendedMicro, 10)
		key := ads.RtbFloorRedisKeyPrefix + deal.DealID
		for _, rdb := range s.rdbs {
			if rdb == nil {
				continue
			}
			if err := rdb.Set(ctx, key, val, 0).Err(); err != nil {
				return recs, fmt.Errorf("write %s: %w", key, err)
			}
		}
	}

	slog.Info("bid floor optimizer tick complete", "deals", len(recs))
	return recs, nil
}

package management

import (
	"context"
	"fmt"
	"math"
	"time"

	"espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type mabCreativeStat struct {
	impressions int64
	clicks      int64
}

// optimizeBrandCreativeMABTx updates creative weights from ClickHouse CTR and returns brands needing Redis sync (M5.7).
func (s *Service) optimizeBrandCreativeMABTx(ctx context.Context, tx pgx.Tx) ([]uuid.UUID, error) {
	if s.ch == nil {
		return nil, nil
	}
	minImps := s.cfg.MABMinImpressions
	if minImps <= 0 {
		minImps = 1000
	}
	lookbackDays := s.cfg.MABLookbackDays
	if lookbackDays <= 0 {
		lookbackDays = 90
	}

	q := db.New(tx)
	brandRows, err := q.ListDistinctBrandsWithActiveCreatives(ctx)
	if err != nil {
		return nil, fmt.Errorf("list brands for mab: %w", err)
	}
	if len(brandRows) == 0 {
		return nil, nil
	}

	lookbackEnd := time.Now().UTC()
	lookbackStart := lookbackEnd.Add(-time.Duration(lookbackDays) * 24 * time.Hour)
	chStats, err := s.queryMABCreativeStats(ctx, lookbackStart, lookbackEnd)
	if err != nil {
		return nil, err
	}

	var updatedBrands []uuid.UUID
	for _, brandRow := range brandRows {
		brandID := uuid.UUID(brandRow.Bytes)
		creatives, err := q.ListActiveBrandCreatives(ctx, brandRow)
		if err != nil {
			return nil, err
		}
		if len(creatives) < 2 {
			continue
		}

		campaignRows, err := q.ListCampaignIDsByBrand(ctx, brandRow)
		if err != nil {
			return nil, err
		}

		attributed := attributeMABStats(creatives, campaignRows, chStats, minImps)
		if !attributed.anyEligible {
			continue
		}

		newWeights := computeMABWeights(attributed.perCreative)
		brandChanged := false
		for _, cr := range creatives {
			creativeID := uuid.UUID(cr.ID.Bytes)
			newWeight, ok := newWeights[creativeID]
			if !ok || newWeight == cr.Weight {
				continue
			}
			_, err := q.UpdateBrandCreative(ctx, db.UpdateBrandCreativeParams{
				ID:         cr.ID,
				Name:       cr.Name,
				LandingUrl: cr.LandingUrl,
				Weight:     newWeight,
				Status:     cr.Status,
			})
			if err != nil {
				return nil, fmt.Errorf("update creative weight %s: %w", creativeID, err)
			}
			brandChanged = true
		}
		if brandChanged {
			updatedBrands = append(updatedBrands, brandID)
		}
	}
	return updatedBrands, nil
}

type mabAttribution struct {
	perCreative map[uuid.UUID]mabCreativeStat
	anyEligible bool
}

func attributeMABStats(
	creatives []db.BrandCreative,
	campaignRows []pgtype.UUID,
	chStats map[uuid.UUID]mabCreativeStat,
	minImps int64,
) mabAttribution {
	out := mabAttribution{perCreative: make(map[uuid.UUID]mabCreativeStat, len(creatives))}

	for creativeID, stat := range chStats {
		if stat.impressions >= minImps {
			out.perCreative[creativeID] = stat
			out.anyEligible = true
		}
	}
	if out.anyEligible {
		return out
	}

	if len(creatives) == 0 || len(campaignRows) == 0 {
		return out
	}

	var totalImps, totalClicks int64
	for _, camp := range campaignRows {
		if stat, ok := chStats[uuid.UUID(camp.Bytes)]; ok {
			totalImps += stat.impressions
			totalClicks += stat.clicks
		}
	}
	if totalImps < minImps {
		return out
	}

	shareImps := totalImps / int64(len(creatives))
	shareClicks := totalClicks / int64(len(creatives))
	if shareImps < minImps {
		return out
	}

	for _, cr := range creatives {
		creativeID := uuid.UUID(cr.ID.Bytes)
		out.perCreative[creativeID] = mabCreativeStat{
			impressions: shareImps,
			clicks:      shareClicks,
		}
	}
	out.anyEligible = true
	return out
}

func computeMABWeights(stats map[uuid.UUID]mabCreativeStat) map[uuid.UUID]int32 {
	weights := make(map[uuid.UUID]int32, len(stats))
	var sumCTR float64
	for _, stat := range stats {
		if stat.impressions > 0 {
			sumCTR += float64(stat.clicks) / float64(stat.impressions)
		}
	}
	if sumCTR <= 0 {
		for id := range stats {
			weights[id] = 1
		}
		return weights
	}
	for id, stat := range stats {
		ctr := float64(stat.clicks) / float64(stat.impressions)
		w := int32(math.Max(1, math.Round(100*ctr/sumCTR)))
		weights[id] = w
	}
	return weights
}

func (s *Service) queryMABCreativeStats(ctx context.Context, from, to time.Time) (map[uuid.UUID]mabCreativeStat, error) {
	out := make(map[uuid.UUID]mabCreativeStat)

	impQuery := `
SELECT
    toString(campaign_id) AS campaign_id,
    nullIf(JSONExtractString(payload, 'creative_id'), '') AS creative_id,
    count() AS impressions
FROM impressions
WHERE created_at >= ? AND created_at < ?
GROUP BY campaign_id, creative_id`

	impRows, err := s.ch.Query(ctx, impQuery, from, to)
	if err != nil {
		return nil, fmt.Errorf("mab impressions query: %w", err)
	}
	defer impRows.Close()

	type key struct {
		campaignID string
		creativeID string
	}
	imps := make(map[key]int64)
	for impRows.Next() {
		var campaignID, creativeID string
		var impressions uint64
		if err := impRows.Scan(&campaignID, &creativeID, &impressions); err != nil {
			return nil, err
		}
		imps[key{campaignID: campaignID, creativeID: creativeID}] = int64(impressions)
	}
	if err := impRows.Err(); err != nil {
		return nil, err
	}

	clickQuery := `
SELECT
    toString(campaign_id) AS campaign_id,
    nullIf(JSONExtractString(payload, 'creative_id'), '') AS creative_id,
    count() AS clicks
FROM clicks
WHERE created_at >= ? AND created_at < ?
GROUP BY campaign_id, creative_id`

	clickRows, err := s.ch.Query(ctx, clickQuery, from, to)
	if err != nil {
		return nil, fmt.Errorf("mab clicks query: %w", err)
	}
	defer clickRows.Close()

	for clickRows.Next() {
		var campaignID, creativeID string
		var clicks uint64
		if err := clickRows.Scan(&campaignID, &creativeID, &clicks); err != nil {
			return nil, err
		}
		k := key{campaignID: campaignID, creativeID: creativeID}
		statKey, err := mabStatKey(campaignID, creativeID)
		if err != nil {
			continue
		}
		stat := out[statKey]
		stat.clicks = int64(clicks)
		stat.impressions = imps[k]
		out[statKey] = stat
	}
	if err := clickRows.Err(); err != nil {
		return nil, err
	}

	for k, impCount := range imps {
		statKey, err := mabStatKey(k.campaignID, k.creativeID)
		if err != nil {
			continue
		}
		if _, ok := out[statKey]; !ok {
			out[statKey] = mabCreativeStat{impressions: impCount}
		}
	}
	return out, nil
}

func mabStatKey(campaignID, creativeID string) (uuid.UUID, error) {
	if creativeID != "" {
		return uuid.Parse(creativeID)
	}
	return uuid.Parse(campaignID)
}

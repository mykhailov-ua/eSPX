package management

import (
	"context"
	"fmt"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const clickHouseStaleThreshold = 5 * time.Minute

// CampaignMetricsDTO holds Postgres campaign_stats counters for a date range.
type CampaignMetricsDTO struct {
	Impressions int64 `json:"impressions"`
	Clicks      int64 `json:"clicks"`
	Conversions int64 `json:"conversions"`
}

// CampaignHourlyBucketDTO is one hourly aggregate from ClickHouse materialized views.
type CampaignHourlyBucketDTO struct {
	Hour        string `json:"hour"`
	Impressions int64  `json:"impressions"`
	Clicks      int64  `json:"clicks"`
	Conversions int64  `json:"conversions"`
}

// CampaignStatsDTO is the merged Postgres + ClickHouse stats payload for /api/v1/campaigns/{id}/stats.
type CampaignStatsDTO struct {
	CampaignID   string                    `json:"campaign_id"`
	CurrentSpend string                    `json:"current_spend"`
	Metrics      CampaignMetricsDTO        `json:"metrics"`
	Hourly       []CampaignHourlyBucketDTO `json:"hourly"`
	Granularity  string                    `json:"granularity"`
	From         string                    `json:"from"`
	To           string                    `json:"to"`
	Stale        bool                      `json:"stale"`
	Consistency  string                    `json:"consistency"`
}

// SetClickHouse attaches an optional analytics connection for reporting endpoints.
func (s *Service) SetClickHouse(conn driver.Conn) {
	s.ch = conn
}

// GetCampaignStats merges Postgres spend and counters with ClickHouse hourly MVs.
func (s *Service) GetCampaignStats(ctx context.Context, campaignID uuid.UUID, from, to time.Time, granularity string) (CampaignStatsDTO, error) {
	if granularity != "hour" {
		return CampaignStatsDTO{}, fmt.Errorf("unsupported granularity: %s", granularity)
	}
	if !to.After(from) {
		return CampaignStatsDTO{}, fmt.Errorf("invalid time range: to must be after from")
	}

	q := db.New(s.GetPool())
	camp, err := q.GetCampaign(ctx, ads.ToUUID(campaignID))
	if err != nil {
		if err == pgx.ErrNoRows {
			return CampaignStatsDTO{}, pgx.ErrNoRows
		}
		return CampaignStatsDTO{}, err
	}

	stats, err := q.SumCampaignStatsInRange(ctx, db.SumCampaignStatsInRangeParams{
		CampaignID: ads.ToUUID(campaignID),
		FromDate:   pgtype.Date{Time: from.UTC(), Valid: true},
		ToDate:     pgtype.Date{Time: to.UTC(), Valid: true},
	})
	if err != nil {
		return CampaignStatsDTO{}, err
	}

	report := CampaignStatsDTO{
		CampaignID:   campaignID.String(),
		CurrentSpend: formatMicro(camp.CurrentSpend),
		Metrics: CampaignMetricsDTO{
			Impressions: stats.Impressions,
			Clicks:      stats.Clicks,
			Conversions: stats.Conversions,
		},
		Hourly:      []CampaignHourlyBucketDTO{},
		Granularity: granularity,
		From:        from.UTC().Format(time.RFC3339),
		To:          to.UTC().Format(time.RFC3339),
		Stale:       false,
		Consistency: "strong",
	}

	if s.ch == nil {
		return report, nil
	}

	hourly, lag, err := s.queryClickHouseHourly(ctx, campaignID, from, to)
	if err != nil {
		return CampaignStatsDTO{}, err
	}
	report.Hourly = hourly
	report.Consistency = "eventual"
	report.Stale = lag > clickHouseStaleThreshold
	return report, nil
}

func (s *Service) queryClickHouseHourly(ctx context.Context, campaignID uuid.UUID, from, to time.Time) ([]CampaignHourlyBucketDTO, time.Duration, error) {
	type row struct {
		hour        time.Time
		impressions uint64
		clicks      uint64
		conversions uint64
	}

	query := `
SELECT
    hour,
    sum(impressions) AS impressions,
    sum(clicks) AS clicks,
    sum(conversions) AS conversions
FROM (
    SELECT hour, impression_count AS impressions, toUInt64(0) AS clicks, toUInt64(0) AS conversions
    FROM mv_campaign_hourly_impressions
    WHERE campaign_id = ? AND hour >= ? AND hour < ?
    UNION ALL
    SELECT hour, toUInt64(0), click_count, toUInt64(0)
    FROM mv_campaign_hourly_clicks
    WHERE campaign_id = ? AND hour >= ? AND hour < ?
    UNION ALL
    SELECT hour, toUInt64(0), toUInt64(0), conversion_count
    FROM mv_campaign_hourly_conversions
    WHERE campaign_id = ? AND hour >= ? AND hour < ?
)
GROUP BY hour
ORDER BY hour`

	rows, err := s.ch.Query(ctx, query,
		campaignID, from.UTC(), to.UTC(),
		campaignID, from.UTC(), to.UTC(),
		campaignID, from.UTC(), to.UTC(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("clickhouse hourly query: %w", err)
	}
	defer rows.Close()

	buckets := make([]CampaignHourlyBucketDTO, 0)
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.hour, &r.impressions, &r.clicks, &r.conversions); err != nil {
			return nil, 0, fmt.Errorf("clickhouse hourly scan: %w", err)
		}
		buckets = append(buckets, CampaignHourlyBucketDTO{
			Hour:        r.hour.UTC().Format(time.RFC3339),
			Impressions: int64(r.impressions),
			Clicks:      int64(r.clicks),
			Conversions: int64(r.conversions),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	lag, err := s.clickHouseIngestionLag(ctx)
	if err != nil {
		return nil, 0, err
	}
	return buckets, lag, nil
}

func (s *Service) clickHouseIngestionLag(ctx context.Context) (time.Duration, error) {
	var latest time.Time
	err := s.ch.QueryRow(ctx, `
SELECT max(latest) FROM (
    SELECT max(created_at) AS latest FROM impressions
    UNION ALL
    SELECT max(created_at) FROM clicks
    UNION ALL
    SELECT max(created_at) FROM conversions
)`).Scan(&latest)
	if err != nil {
		return 0, fmt.Errorf("clickhouse lag probe: %w", err)
	}
	if latest.IsZero() {
		return clickHouseStaleThreshold + time.Second, nil
	}
	return time.Since(latest.UTC()), nil
}

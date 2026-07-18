package adminapi

import (
	"context"
	"fmt"
	"time"

	billingdb "espx/internal/billing/db"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgtype"
)

const billingForecastCHTimeout = 1500 * time.Millisecond

// ForecastDTO projects month-end spend from ledger run-rate and ClickHouse hourly activity.
type ForecastDTO struct {
	CustomerID               string          `json:"customer_id"`
	Month                    string          `json:"month"`
	LedgerMTDMicro           int64           `json:"ledger_mtd_micro"`
	LedgerRunRateMicroPerDay int64           `json:"ledger_run_rate_micro_per_day"`
	CHHourlyImpressions      []CHHourlyPoint `json:"ch_hourly_impressions,omitempty"`
	ProjectedMonthEndMicro   int64           `json:"projected_month_end_micro"`
	DaysRemaining            int             `json:"days_remaining"`
	LowConfidence            bool            `json:"low_confidence"`
	CHUnavailable            bool            `json:"ch_unavailable,omitempty"`
}

// CHHourlyPoint is one hour bucket from ClickHouse MVs.
type CHHourlyPoint struct {
	Hour        string `json:"hour"`
	Impressions int64  `json:"impressions"`
}

// WithClickHouse attaches an optional ClickHouse connection for billing forecast.
func (s *CompositeReadService) WithClickHouse(ch driver.Conn) *CompositeReadService {
	if s == nil {
		return nil
	}
	s.ch = ch
	return s
}

// BuildForecast estimates projected month-end spend for one customer.
func (s *CompositeReadService) BuildForecast(ctx context.Context, customerID uuid.UUID) (ForecastDTO, error) {
	if s == nil || s.pool == nil {
		return ForecastDTO{}, fmt.Errorf("composite read service not configured")
	}

	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, 0)
	daysRemaining := int(monthEnd.Sub(now).Hours()/24) + 1
	if daysRemaining < 0 {
		daysRemaining = 0
	}

	pgCustomer := pgtype.UUID{Bytes: customerID, Valid: true}
	mtd, err := s.queries.SumCustomerSpendInWindow(ctx, billingdb.SumCustomerSpendInWindowParams{
		CustomerID:  pgCustomer,
		CreatedAt:   pgTimestamp(monthStart),
		CreatedAt_2: pgTimestamp(now),
	})
	if err != nil {
		return ForecastDTO{}, err
	}

	spend7d, err := s.queries.SumCustomerSpendLast7Days(ctx, pgCustomer)
	if err != nil {
		return ForecastDTO{}, err
	}
	runRate := spend7d / 7
	if runRate < 0 {
		runRate = 0
	}

	out := ForecastDTO{
		CustomerID:               customerID.String(),
		Month:                    monthStart.Format("2006-01"),
		LedgerMTDMicro:           mtd,
		LedgerRunRateMicroPerDay: runRate,
		DaysRemaining:            daysRemaining,
		ProjectedMonthEndMicro:   mtd + runRate*int64(daysRemaining),
	}

	if s.ch == nil {
		out.LowConfidence = true
		out.CHUnavailable = true
		return out, nil
	}

	campaignIDs, err := s.customerCampaignIDs(ctx, customerID)
	if err != nil {
		return ForecastDTO{}, err
	}
	if len(campaignIDs) == 0 {
		out.LowConfidence = true
		return out, nil
	}

	chCtx, cancel := context.WithTimeout(ctx, billingForecastCHTimeout)
	defer cancel()

	lookback := now.Add(-7 * 24 * time.Hour)
	points, chErr := s.queryCHHourlyImpressions(chCtx, lookback, now, campaignIDs)
	if chErr != nil {
		out.LowConfidence = true
		out.CHUnavailable = true
		return out, nil
	}
	out.CHHourlyImpressions = points
	if len(points) == 0 {
		out.LowConfidence = true
	}
	return out, nil
}

func (s *CompositeReadService) customerCampaignIDs(ctx context.Context, customerID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM campaigns
		WHERE customer_id = $1 AND deleted_at IS NULL`, customerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id pgtype.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, uuid.UUID(id.Bytes))
	}
	return ids, rows.Err()
}

func (s *CompositeReadService) queryCHHourlyImpressions(ctx context.Context, from, to time.Time, campaignIDs []uuid.UUID) ([]CHHourlyPoint, error) {
	if s.ch == nil {
		return nil, fmt.Errorf("clickhouse not configured")
	}
	query := `
SELECT toStartOfHour(hour) AS hr, sum(impression_count) AS impressions
FROM mv_campaign_hourly_impressions
WHERE hour >= ? AND hour < ? AND campaign_id IN (?)
GROUP BY hr
ORDER BY hr`
	rows, err := s.ch.Query(ctx, query, from, to, campaignIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]CHHourlyPoint, 0, 168)
	for rows.Next() {
		var hr time.Time
		var impressions uint64
		if err := rows.Scan(&hr, &impressions); err != nil {
			return nil, err
		}
		out = append(out, CHHourlyPoint{
			Hour:        hr.UTC().Format(time.RFC3339),
			Impressions: int64(impressions),
		})
	}
	return out, rows.Err()
}

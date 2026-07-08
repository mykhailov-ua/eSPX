package management

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"espx/internal/ads/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	forecastLookbackDays         = 90
	forecastCHQueryTimeout       = 1500 * time.Millisecond
	forecastMinSampleImpressions = int64(1000)
	forecastUnderfillAdvisoryPct = 0.20
	forecastDefaultRetryAfterSec = 30
	forecastMaxSpendCurvePoints  = 2160
)

var (
	ErrForecastClickHouseTimeout = errors.New("forecast clickhouse query timed out")
	ErrForecastUnavailable       = errors.New("forecast service unavailable")
	ErrClickHouseNotConfigured   = errors.New("clickhouse not configured")
)

// CampaignForecastInput is the planning request for POST /api/v1/forecast/campaign.
type CampaignForecastInput struct {
	CustomerID       *uuid.UUID
	BudgetLimitMicro int64
	TargetCountries  []string
	DaypartHours     []int16
	StartAt          time.Time
	EndAt            time.Time
	PacingMode       string
	Timezone         string
}

// SpendCurvePoint is one hour in the projected spend distribution.
type SpendCurvePoint struct {
	Hour        string `json:"hour"`
	SpendMicro  int64  `json:"spend_micro"`
	Impressions int64  `json:"impressions"`
}

// ForecastAdvisory is an optional non-binding recommendation (M5.4).
type ForecastAdvisory struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	SuggestedPacing string `json:"suggested_pacing"`
}

// CampaignForecastDTO is the forecast response payload.
type CampaignForecastDTO struct {
	ImpressionsP50 int64             `json:"impressions_p50"`
	ImpressionsP90 int64             `json:"impressions_p90"`
	SpendCurve     []SpendCurvePoint `json:"spend_curve"`
	LowConfidence  bool              `json:"low_confidence"`
	Advisory       *ForecastAdvisory `json:"advisory,omitempty"`
}

type forecastHourlySample struct {
	hourOfDay   int
	impressions uint64
}

// ForecastCampaign estimates delivery for a planned campaign using ClickHouse hourly MVs (M5.1–M5.4).
func (s *Service) ForecastCampaign(ctx context.Context, in CampaignForecastInput) (CampaignForecastDTO, error) {
	if s.ch == nil {
		return CampaignForecastDTO{}, ErrClickHouseNotConfigured
	}
	if in.BudgetLimitMicro <= 0 {
		return CampaignForecastDTO{}, errValidation("budget_limit_micro must be greater than zero")
	}
	if !in.EndAt.After(in.StartAt) {
		return CampaignForecastDTO{}, ErrInvalidTimeRange
	}
	pacing := normalizeForecastPacing(in.PacingMode)

	chCtx, cancel := context.WithTimeout(ctx, forecastCHQueryTimeout)
	defer cancel()

	lookbackEnd := time.Now().UTC().Truncate(time.Hour)
	lookbackStart := lookbackEnd.Add(-forecastLookbackDays * 24 * time.Hour)

	campaignIDs, err := s.forecastCampaignIDs(chCtx, in.CustomerID)
	if err != nil {
		return CampaignForecastDTO{}, err
	}

	totalSample, hourlySamples, err := s.queryForecastHourlySamples(chCtx, lookbackStart, lookbackEnd, campaignIDs)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(chCtx.Err(), context.DeadlineExceeded) {
			return CampaignForecastDTO{}, ErrForecastClickHouseTimeout
		}
		return CampaignForecastDTO{}, fmt.Errorf("%w: %w", ErrForecastUnavailable, err)
	}

	lowConfidence := int64(totalSample) < forecastMinSampleImpressions
	hourWeights := buildHourWeights(hourlySamples)
	activeHours := enumerateActiveHours(in.StartAt, in.EndAt, in.DaypartHours, in.Timezone)

	flightImpressions := projectFlightImpressions(hourWeights, activeHours, totalSample)
	p50, p90 := impressionPercentiles(hourlySamples, activeHours, totalSample)

	cpmMicro := impliedCPMMicro(in.BudgetLimitMicro, flightImpressions)
	spendCurve := buildSpendCurve(activeHours, in.BudgetLimitMicro, pacing, cpmMicro)

	out := CampaignForecastDTO{
		ImpressionsP50: p50,
		ImpressionsP90: p90,
		SpendCurve:     spendCurve,
		LowConfidence:  lowConfidence,
	}
	if advisory := evenPacingAdvisory(pacing, in.BudgetLimitMicro, p50, cpmMicro); advisory != nil {
		out.Advisory = advisory
	}
	_ = in.TargetCountries
	return out, nil
}

func normalizeForecastPacing(mode string) string {
	switch mode {
	case "EVEN", "even":
		return "EVEN"
	default:
		return "ASAP"
	}
}

func (s *Service) forecastCampaignIDs(ctx context.Context, customerID *uuid.UUID) ([]uuid.UUID, error) {
	if customerID == nil || *customerID == uuid.Nil {
		return nil, nil
	}
	q := db.New(s.GetPool())
	rows, err := q.ListCampaigns(ctx, db.ListCampaignsParams{
		CustomerID: pgtype.UUID{Bytes: *customerID, Valid: true},
		Limit:      500,
		Offset:     0,
	})
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, uuid.UUID(row.ID.Bytes))
	}
	return ids, nil
}

func (s *Service) queryForecastHourlySamples(ctx context.Context, from, to time.Time, campaignIDs []uuid.UUID) (uint64, []forecastHourlySample, error) {
	var (
		query string
		args  []any
	)
	if len(campaignIDs) == 0 {
		query = `
SELECT toHour(hour) AS hr, sum(impression_count) AS impressions
FROM mv_campaign_hourly_impressions
WHERE hour >= ? AND hour < ?
GROUP BY hr
ORDER BY hr`
		args = []any{from, to}
	} else {
		query = `
SELECT toHour(hour) AS hr, sum(impression_count) AS impressions
FROM mv_campaign_hourly_impressions
WHERE hour >= ? AND hour < ? AND campaign_id IN (?)
GROUP BY hr
ORDER BY hr`
		args = []any{from, to, campaignIDs}
	}

	rows, err := s.ch.Query(ctx, query, args...)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	var total uint64
	samples := make([]forecastHourlySample, 0, 24)
	for rows.Next() {
		var sample forecastHourlySample
		if err := rows.Scan(&sample.hourOfDay, &sample.impressions); err != nil {
			return 0, nil, err
		}
		total += sample.impressions
		samples = append(samples, sample)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return total, samples, nil
}

func buildHourWeights(samples []forecastHourlySample) [24]float64 {
	var weights [24]float64
	if len(samples) == 0 {
		for i := range weights {
			weights[i] = 1.0 / 24.0
		}
		return weights
	}
	var sum float64
	for _, s := range samples {
		if s.hourOfDay >= 0 && s.hourOfDay < 24 {
			weights[s.hourOfDay] = float64(s.impressions)
			sum += float64(s.impressions)
		}
	}
	if sum <= 0 {
		for i := range weights {
			weights[i] = 1.0 / 24.0
		}
		return weights
	}
	for i := range weights {
		weights[i] /= sum
	}
	return weights
}

func enumerateActiveHours(start, end time.Time, daypart []int16, timezone string) []time.Time {
	loc := time.UTC
	if timezone != "" {
		if l, err := time.LoadLocation(timezone); err == nil {
			loc = l
		}
	}
	daypartSet := make(map[int16]struct{}, len(daypart))
	for _, h := range daypart {
		daypartSet[h] = struct{}{}
	}
	useDaypart := len(daypartSet) > 0

	start = start.In(loc).Truncate(time.Hour)
	end = end.In(loc).Truncate(time.Hour)
	if !end.After(start) {
		return nil
	}

	hours := make([]time.Time, 0, int(end.Sub(start)/time.Hour))
	for t := start; t.Before(end); t = t.Add(time.Hour) {
		if len(hours) >= forecastMaxSpendCurvePoints {
			break
		}
		if useDaypart {
			if _, ok := daypartSet[int16(t.Hour())]; !ok {
				continue
			}
		}
		hours = append(hours, t.UTC())
	}
	return hours
}

func projectFlightImpressions(weights [24]float64, activeHours []time.Time, totalSample uint64) int64 {
	if len(activeHours) == 0 {
		return 0
	}
	lookbackHours := float64(forecastLookbackDays * 24)
	avgPerHour := float64(totalSample) / lookbackHours
	if avgPerHour <= 0 {
		return 0
	}
	var weighted float64
	for _, h := range activeHours {
		weighted += weights[h.Hour()] * 24.0
	}
	return int64(math.Round(avgPerHour * weighted))
}

func impressionPercentiles(samples []forecastHourlySample, activeHours []time.Time, totalSample uint64) (p50, p90 int64) {
	values := make([]int64, 0, len(activeHours))
	weights := buildHourWeights(samples)
	lookbackHours := float64(forecastLookbackDays * 24)
	avgPerHour := float64(totalSample) / lookbackHours
	for _, h := range activeHours {
		v := avgPerHour * weights[h.Hour()] * 24.0
		if v > 0 {
			values = append(values, int64(math.Round(v)))
		}
	}
	if len(values) == 0 {
		return 0, 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	p50 = values[len(values)*50/100]
	p90 = values[len(values)*90/100]
	if p90 < p50 {
		p90 = p50
	}
	return p50, p90
}

func impliedCPMMicro(budgetMicro, impressions int64) int64 {
	if impressions <= 0 {
		return budgetMicro
	}
	return budgetMicro / impressions
}

func buildSpendCurve(activeHours []time.Time, budgetMicro int64, pacing string, cpmMicro int64) []SpendCurvePoint {
	if len(activeHours) == 0 {
		return []SpendCurvePoint{}
	}
	curve := make([]SpendCurvePoint, 0, len(activeHours))
	if pacing == "EVEN" {
		perHourSpend := budgetMicro / int64(len(activeHours))
		perHourImps := int64(0)
		if cpmMicro > 0 {
			perHourImps = perHourSpend / cpmMicro
		}
		for _, h := range activeHours {
			curve = append(curve, SpendCurvePoint{
				Hour:        h.Format(time.RFC3339),
				SpendMicro:  perHourSpend,
				Impressions: perHourImps,
			})
		}
		return curve
	}

	frontCount := len(activeHours) * 30 / 100
	if frontCount < 1 {
		frontCount = 1
	}
	frontBudget := budgetMicro * 70 / 100
	backBudget := budgetMicro - frontBudget
	frontPer := frontBudget / int64(frontCount)
	backHours := len(activeHours) - frontCount
	var backPer int64
	if backHours > 0 {
		backPer = backBudget / int64(backHours)
	}
	for i, h := range activeHours {
		spend := backPer
		if i < frontCount {
			spend = frontPer
		}
		imps := int64(0)
		if cpmMicro > 0 {
			imps = spend / cpmMicro
		}
		curve = append(curve, SpendCurvePoint{
			Hour:        h.Format(time.RFC3339),
			SpendMicro:  spend,
			Impressions: imps,
		})
	}
	return curve
}

func evenPacingAdvisory(pacing string, budgetMicro, impressionsP50, cpmMicro int64) *ForecastAdvisory {
	if pacing != "EVEN" || budgetMicro <= 0 || impressionsP50 <= 0 || cpmMicro <= 0 {
		return nil
	}
	deliverableSpend := impressionsP50 * cpmMicro
	if deliverableSpend >= budgetMicro {
		return nil
	}
	underfill := float64(budgetMicro-deliverableSpend) / float64(budgetMicro)
	if underfill <= forecastUnderfillAdvisoryPct {
		return nil
	}
	return &ForecastAdvisory{
		Code:            "PACING_UNDERFILL",
		Message:         fmt.Sprintf("EVEN pacing may under-deliver by %.0f%% of budget; consider ASAP for full spend in the flight window", underfill*100),
		SuggestedPacing: "ASAP",
	}
}

// ForecastRetryAfterSec returns the Retry-After hint for forecast 503 responses.
func ForecastRetryAfterSec() int {
	return forecastDefaultRetryAfterSec
}

package management

import (
	"testing"
	"time"

	"espx/internal/config"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSmartPacingExpectedRatio_daypartWeighted(t *testing.T) {
	t.Parallel()
	var weights [24]float64
	for h := 9; h <= 17; h++ {
		weights[h] = 1.0
	}
	for h := 0; h < 24; h++ {
		if weights[h] == 0 {
			weights[h] = 0.01
		}
	}

	loc := time.UTC
	daypart := []int16{9, 10, 11, 12, 13, 14, 15, 16, 17}
	midday := time.Date(2026, 7, 7, 13, 30, 0, 0, loc)

	ratio := smartPacingExpectedRatio(weights, daypart, midday)
	assert.InDelta(t, 0.5, ratio, 0.15)

	linear := smartPacingExpectedRatio(uniformHourWeights(), nil, midday)
	assert.InDelta(t, 0.5625, linear, 0.02)
}

func TestDeliveryOutboxMerge_priority(t *testing.T) {
	t.Parallel()
	campID := uuid.New()
	merge := make(deliveryOutboxMerge)

	merge.upsert(campID, outboxPriCreateCampaign, "CREATE_CAMPAIGN", []byte(`{"campaign_id":"x"}`))
	merge.upsert(campID, outboxPriPacing, "UPDATE_CAMPAIGN_PACING", []byte(`{"campaign_id":"x","pacing_mode":"EVEN"}`))
	merge.upsert(campID, outboxPriCreateCampaign, "CREATE_CAMPAIGN", []byte(`{"campaign_id":"x","budget":1}`))

	entry := merge[campID]
	assert.Equal(t, "UPDATE_CAMPAIGN_PACING", entry.eventType)
	assert.Equal(t, outboxPriPacing, entry.priority)
}

func TestComputeMABWeights_proportionalCTR(t *testing.T) {
	t.Parallel()
	a := uuid.New()
	b := uuid.New()
	stats := map[uuid.UUID]mabCreativeStat{
		a: {impressions: 2000, clicks: 20},
		b: {impressions: 2000, clicks: 10},
	}
	weights := computeMABWeights(stats)
	assert.Greater(t, weights[a], weights[b])
	assert.Greater(t, weights[b], int32(0))
}

func TestCalculateOverdraft_reconLagPenalty(t *testing.T) {
	t.Parallel()
	cfg := configWithReconPenalty()
	worker := &CreditScoringWorker{svc: &Service{cfg: cfg}}
	base := worker.calculateOverdraft(40, 1_000_000_000, 0)
	penalized := worker.calculateOverdraft(40, 1_000_000_000, 500_000_000)
	assert.Equal(t, base/2, penalized)
}

func configWithReconPenalty() *config.Config {
	cfg := &config.Config{
		CreditScoringMinAgeDays:         7,
		CreditScoringMatureAgeDays:      30,
		CreditScoringMaturePercent:      30,
		CreditScoringMaxCap:             10_000_000_000,
		CreditScoringReconLagThreshold:  100_000_000,
		CreditScoringReconLagPenaltyPct: 50,
	}
	return cfg
}

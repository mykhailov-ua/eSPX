package filter

import (
	"testing"

	"espx/internal/domain"

	"github.com/stretchr/testify/assert"
)

func TestMapFraudTier_defaults(t *testing.T) {
	assert.Equal(t, FraudTierPass, MapFraudTier(10, 0, 0, 0, 0))
	assert.Equal(t, FraudTierSuspect, MapFraudTier(45, 30, 60, 80, 100))
	assert.Equal(t, FraudTierIVT, MapFraudTier(70, 30, 60, 80, 100))
	assert.Equal(t, FraudTierBlock, MapFraudTier(90, 30, 60, 80, 100))
}

func TestFraudAccumulator_scoreAndReason(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonDatacenterIP)
	acc.add(FraudReasonLowTTC)
	assert.Equal(t, uint32(90), acc.score)

	evt := &domain.Event{StringBuffer: make([]byte, 0, 64)}
	tier := applyFraudAccumulatorForCampaign(evt, acc, nil)
	assert.Equal(t, FraudTierBlock, tier)
	assert.Equal(t, uint32(90), evt.FraudScore)
	assert.Equal(t, "datacenter_ip,low_ttc", evt.FraudReason)
}

func TestFraudAccumulator_dedupesSignals(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonLowTTC)
	acc.add(FraudReasonLowTTC)
	assert.Equal(t, uint8(1), acc.count)
	assert.Equal(t, uint32(45), acc.score)
}

func TestMapFraudTier_campaignThresholds(t *testing.T) {
	camp := &domain.Campaign{
		FraudThresholdPass:    20,
		FraudThresholdSuspect: 40,
		FraudThresholdIVT:     60,
		FraudThresholdBlock:   100,
	}
	pass, suspect, ivt, block := fraudThresholdsFromCampaign(camp)
	assert.Equal(t, FraudTierSuspect, MapFraudTier(25, pass, suspect, ivt, block))
}

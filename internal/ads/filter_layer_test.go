package ads

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecideFraudLayer_L3(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonL3Blocklist)
	assert.Equal(t, FraudLayerL1Reject, decideFraudLayer(acc, FraudTierBlock))
}

func TestDecideFraudLayer_dualL1(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonDatacenterIP)
	acc.add(FraudReasonLowTTC)
	assert.Equal(t, FraudLayerL1Reject, decideFraudLayer(acc, FraudTierBlock))
}

func TestDecideFraudLayer_singleL1Shadow(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonDatacenterIP)
	assert.Equal(t, FraudLayerL2Shadow, decideFraudLayer(acc, FraudTierIVT))
}

func TestDecideFraudLayer_weakSignalShadow(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonMissingImpTS)
	assert.Equal(t, FraudLayerL2Shadow, decideFraudLayer(acc, FraudTierSuspect))
}

func TestFraudAccumulator_shortCircuitBudget(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonDatacenterIP)
	assert.False(t, acc.shouldShortCircuitFraudBudget())

	acc.add(FraudReasonLowTTC)
	assert.True(t, acc.shouldShortCircuitFraudBudget())

	acc.reset()
	acc.add(FraudReasonL3Blocklist)
	assert.True(t, acc.shouldShortCircuitFraudBudget())
}

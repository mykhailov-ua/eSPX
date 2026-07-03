package ads

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreboundFraudMetrics_tierAndReason(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonDatacenterIP)

	beforeTier := testutil.ToFloat64(boundFraudMetrics.tierIVT)
	beforeReason := testutil.ToFloat64(boundFraudMetrics.reason[FraudReasonDatacenterIP])

	recordFraudMetrics(acc, FraudTierIVT, FraudLayerL2Shadow)

	assert.Equal(t, beforeTier+1, testutil.ToFloat64(boundFraudMetrics.tierIVT))
	assert.Equal(t, beforeReason+1, testutil.ToFloat64(boundFraudMetrics.reason[FraudReasonDatacenterIP]))
}

func TestPreboundFraudMetrics_l1Reject(t *testing.T) {
	acc := &fraudAccumulator{}
	acc.add(FraudReasonDatacenterIP)
	acc.add(FraudReasonLowTTC)

	before := testutil.ToFloat64(boundFraudMetrics.l1Reject)
	recordFraudMetrics(acc, FraudTierBlock, FraudLayerL1Reject)
	assert.Equal(t, before+1, testutil.ToFloat64(boundFraudMetrics.l1Reject))
}

func TestPreboundFraudMetrics_skipsEmptyAccumulator(t *testing.T) {
	before := testutil.ToFloat64(boundFraudMetrics.l1Reject)
	recordFraudMetrics(nil, FraudTierPass, FraudLayerNone)
	recordFraudMetrics(&fraudAccumulator{}, FraudTierPass, FraudLayerNone)
	assert.Equal(t, before, testutil.ToFloat64(boundFraudMetrics.l1Reject))
}

func TestPreboundFraudMetrics_allReasonLabelsBound(t *testing.T) {
	pm := newPreboundFraudMetrics()
	require.NotNil(t, pm.reason[FraudReasonDatacenterIP])
	require.NotNil(t, pm.reason[FraudReasonLowTTC])
	require.NotNil(t, pm.reason[FraudReasonMissingImpTS])
	require.NotNil(t, pm.reason[FraudReasonL3Blocklist])
}

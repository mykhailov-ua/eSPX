package ingestion

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFraudReasonRegistry_stableCodes(t *testing.T) {
	assert.Equal(t, "datacenter_ip", FraudReasonCode(FraudReasonDatacenterIP))
	assert.Equal(t, "low_ttc", FraudReasonCode(FraudReasonLowTTC))
	assert.Equal(t, "missing_imp_ts", FraudReasonCode(FraudReasonMissingImpTS))
	assert.Equal(t, uint8(45), FraudSignalWeight(FraudReasonDatacenterIP))
}

package management

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateFraudThresholds(t *testing.T) {
	require.NoError(t, validateFraudThresholds(30, 60, 80, 100))
	require.Error(t, validateFraudThresholds(60, 30, 80, 100))
	require.Error(t, validateFraudThresholds(30, 60, 80, 101))
}

func TestResolveFraudThresholds_defaultsWhenNil(t *testing.T) {
	pass, suspect, ivt, block := ResolveFraudThresholds(nil)
	assert.Equal(t, uint8(30), pass)
	assert.Equal(t, uint8(60), suspect)
	assert.Equal(t, uint8(80), ivt)
	assert.Equal(t, uint8(100), block)
}

package money

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDecimal(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"12", 12_000_000},
		{"12.34", 12_340_000},
		{"0.000001", 1},
		{"-1.5", -1_500_000},
	}
	for _, tc := range tests {
		got, err := ParseDecimal(tc.in)
		require.NoError(t, err)
		assert.Equal(t, tc.want, got, tc.in)
	}
}

func TestLegacyFloatToMicro(t *testing.T) {
	got, err := LegacyFloatToMicro(10.5)
	require.NoError(t, err)
	assert.Equal(t, int64(10_500_000), got)
}

func TestPercentBps(t *testing.T) {
	assert.Equal(t, int64(250_000), PercentBps(10_000_000, 250))
}

func TestScalePPM(t *testing.T) {
	assert.Equal(t, int64(500_000), ScalePPM(1_000_000, 500_000))
}

func TestFormatDecimal(t *testing.T) {
	assert.Equal(t, "10.5", FormatDecimal(10_500_000))
	assert.Equal(t, "0", FormatDecimal(0))
	assert.Equal(t, "-1.25", FormatDecimal(-1_250_000))
}

func TestJSONAmountToMicro(t *testing.T) {
	got, err := JSONAmountToMicro(1.23)
	require.NoError(t, err)
	assert.Equal(t, int64(1_230_000), got)
}

func TestMulMicro(t *testing.T) {
	assert.Equal(t, int64(5_000_000), MulMicro(50_000, 100))
}

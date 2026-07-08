package management

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForecast_evenPacingAdvisory_underfill(t *testing.T) {
	t.Parallel()
	adv := evenPacingAdvisory("EVEN", 10_000_000, 1_000, 5_000)
	require.NotNil(t, adv)
	assert.Equal(t, "PACING_UNDERFILL", adv.Code)
	assert.Equal(t, "ASAP", adv.SuggestedPacing)
}

func TestForecast_evenPacingAdvisory_noAdvisory_when_deliverable(t *testing.T) {
	t.Parallel()
	adv := evenPacingAdvisory("EVEN", 10_000_000, 10_000, 1_000)
	assert.Nil(t, adv)
}

func TestForecast_evenPacingAdvisory_skipped_for_ASAP(t *testing.T) {
	t.Parallel()
	adv := evenPacingAdvisory("ASAP", 10_000_000, 1_000, 5_000)
	assert.Nil(t, adv)
}

func TestForecast_enumerateActiveHours_daypart(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	hours := enumerateActiveHours(start, end, []int16{9, 10, 11}, "UTC")
	assert.Len(t, hours, 3)
}

func TestForecast_buildSpendCurve_EVEN_deterministic(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	hours := enumerateActiveHours(start, start.Add(4*time.Hour), nil, "UTC")
	curve := buildSpendCurve(hours, 4_000_000, "EVEN", 1_000)
	require.Len(t, curve, 4)
	for _, p := range curve {
		assert.Equal(t, int64(1_000_000), p.SpendMicro)
		assert.Equal(t, int64(1_000), p.Impressions)
	}
}

func TestForecast_impressionPercentiles_deterministic(t *testing.T) {
	t.Parallel()
	samples := []forecastHourlySample{
		{hourOfDay: 10, impressions: 100},
		{hourOfDay: 11, impressions: 200},
		{hourOfDay: 12, impressions: 300},
	}
	start := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	hours := enumerateActiveHours(start, start.Add(3*time.Hour), nil, "UTC")
	p50a, p90a := impressionPercentiles(samples, hours, 600)
	p50b, p90b := impressionPercentiles(samples, hours, 600)
	assert.Equal(t, p50a, p50b)
	assert.Equal(t, p90a, p90b)
}

func TestForecast_lowConfidence_threshold(t *testing.T) {
	t.Parallel()
	assert.True(t, int64(999) < forecastMinSampleImpressions)
	assert.False(t, int64(1000) < forecastMinSampleImpressions)
}

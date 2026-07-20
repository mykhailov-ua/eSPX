package ivtdetector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPopVariance_timerBot(t *testing.T) {
	deltas := make([]float64, 35)
	for i := range deltas {
		deltas[i] = 1.0
	}
	assert.InDelta(t, 0.0, popVariance(deltas), 1e-9)
	assert.True(t, isIntervalBot(deltas, 30, 0.005))
}

func TestPopVariance_humanTraffic(t *testing.T) {
	deltas := []float64{
		0.4, 2.8, 15.2, 0.9, 7.1, 22.0, 1.3, 5.6, 11.4, 0.7,
		3.2, 18.5, 2.1, 9.8, 0.5, 14.0, 4.4, 6.7, 1.9, 12.3,
		0.6, 8.2, 3.7, 16.1, 2.5, 10.0, 1.1, 7.9, 4.8, 13.6,
		0.8, 5.1, 19.4, 2.0, 11.7,
	}
	variance := popVariance(deltas)
	assert.GreaterOrEqual(t, variance, 0.005)
	assert.False(t, isIntervalBot(deltas, 30, 0.005))
}

func TestIsIntervalBot_insufficientSamples(t *testing.T) {
	deltas := make([]float64, 29)
	for i := range deltas {
		deltas[i] = 1.0
	}
	assert.False(t, isIntervalBot(deltas, 30, 0.005))
}

func TestIsIntervalBot_boundaryVariance(t *testing.T) {
	// variance of [1.0, 1.1] = 0.0025 < 0.005 → flagged
	assert.True(t, isIntervalBot([]float64{1.0, 1.1}, 2, 0.005))

	// variance of [1.0, 1.2] = 0.01 >= 0.005 → not flagged
	assert.False(t, isIntervalBot([]float64{1.0, 1.2}, 2, 0.005))
}

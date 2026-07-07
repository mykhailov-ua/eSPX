package management

import (
	"testing"

	"espx/internal/config"

	"github.com/stretchr/testify/assert"
)

func TestComputeRecommendedFloor_lowWinRateDecreases(t *testing.T) {
	cfg := &config.Config{
		BidFloorWinRateLow:  0.05,
		BidFloorWinRateHigh: 0.25,
		BidFloorAdjustPct:   10,
		BidFloorMinMicro:    1000,
	}
	out := computeRecommendedFloor(100_000, 0.02, 100, cfg)
	assert.Equal(t, int64(90_000), out)
}

func TestComputeRecommendedFloor_highWinRateIncreases(t *testing.T) {
	cfg := &config.Config{
		BidFloorWinRateLow:  0.05,
		BidFloorWinRateHigh: 0.25,
		BidFloorAdjustPct:   10,
		BidFloorMinMicro:    1000,
	}
	out := computeRecommendedFloor(100_000, 0.40, 200, cfg)
	assert.Equal(t, int64(110_000), out)
}

func TestComputeRecommendedFloor_noSampleKeepsBase(t *testing.T) {
	cfg := &config.Config{BidFloorAdjustPct: 10}
	out := computeRecommendedFloor(50_000, 0.0, 0, cfg)
	assert.Equal(t, int64(50_000), out)
}

func TestComputeRecommendedFloor_respectsMinMicro(t *testing.T) {
	cfg := &config.Config{
		BidFloorWinRateLow:  0.05,
		BidFloorAdjustPct:   90,
		BidFloorMinMicro:    5000,
	}
	out := computeRecommendedFloor(10_000, 0.01, 50, cfg)
	assert.Equal(t, int64(5000), out)
}

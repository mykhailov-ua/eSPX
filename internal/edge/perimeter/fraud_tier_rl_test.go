package perimeter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Guards fraud_score tier mapping matches edge-fraud-tier.lua boundaries.
func TestMapFraudRLTier_boundaries(t *testing.T) {
	tier, _ := MapFraudRLTier(10)
	assert.Equal(t, FraudRLTierPass, tier)
	tier, _ = MapFraudRLTier(45)
	assert.Equal(t, FraudRLTierSuspect, tier)
	tier, _ = MapFraudRLTier(70)
	assert.Equal(t, FraudRLTierIVT, tier)
	tier, _ = MapFraudRLTier(90)
	assert.Equal(t, FraudRLTierBlock, tier)
}

// Guards tier limits scale base campaign RL per config percentages.
func TestTierLimit_scaling(t *testing.T) {
	cfg := DefaultFraudRLConfig()
	assert.Equal(t, 100, TierLimit(FraudRLTierPass, cfg))
	assert.Equal(t, 50, TierLimit(FraudRLTierSuspect, cfg))
	assert.Equal(t, 10, TierLimit(FraudRLTierIVT, cfg))
	assert.Equal(t, 0, TierLimit(FraudRLTierBlock, cfg))
}

// Guards Retry-After seconds increase with fraud tier severity.
func TestRetryAfterSec_tiers(t *testing.T) {
	cfg := DefaultFraudRLConfig()
	assert.Equal(t, 30, RetryAfterSec(FraudRLTierSuspect, cfg))
	assert.Equal(t, 60, RetryAfterSec(FraudRLTierIVT, cfg))
	assert.Equal(t, 120, RetryAfterSec(FraudRLTierBlock, cfg))
}

// Guards score >= 81 maps to immediate edge block tier.
func TestFraudTierBlock_score81(t *testing.T) {
	tier, score := MapFraudRLTier(85)
	assert.Equal(t, FraudRLTierBlock, tier)
	assert.True(t, ShouldBlockTier(tier))
	assert.Equal(t, 0, TierLimit(tier, DefaultFraudRLConfig()))
	assert.Equal(t, 120, RetryAfterSec(tier, DefaultFraudRLConfig()))
	assert.Equal(t, 85, score)
}

package perimeter

// FraudRLTier is the edge rate-limit band derived from fraud_score.
type FraudRLTier string

const (
	FraudRLTierPass    FraudRLTier = "pass"
	FraudRLTierSuspect FraudRLTier = "suspect"
	FraudRLTierIVT     FraudRLTier = "ivt"
	FraudRLTierBlock   FraudRLTier = "block"
)

const (
	defaultFraudRLPassMax    = 30
	defaultFraudRLSuspectMax = 60
	defaultFraudRLIVTMax     = 80
)

// FraudRLConfig mirrors config:values tier RL fields synced by edge-config.lua.
type FraudRLConfig struct {
	BaseLimitPerMin int
	SuspectPct      int
	IVTPct          int
	BlockPct        int
	RetrySuspectSec int
	RetryIVTSec     int
	RetryBlockSec   int
}

// DefaultFraudRLConfig returns production-aligned tier RL defaults.
func DefaultFraudRLConfig() FraudRLConfig {
	return FraudRLConfig{
		BaseLimitPerMin: 100,
		SuspectPct:      50,
		IVTPct:          10,
		BlockPct:        0,
		RetrySuspectSec: 30,
		RetryIVTSec:     60,
		RetryBlockSec:   120,
	}
}

// MapFraudRLTier maps a fraud score to an edge tier.
func MapFraudRLTier(score int) (FraudRLTier, int) {
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	switch {
	case score <= defaultFraudRLPassMax:
		return FraudRLTierPass, score
	case score <= defaultFraudRLSuspectMax:
		return FraudRLTierSuspect, score
	case score <= defaultFraudRLIVTMax:
		return FraudRLTierIVT, score
	default:
		return FraudRLTierBlock, score
	}
}

// TierLimit returns the per-window campaign limit for a tier.
func TierLimit(tier FraudRLTier, cfg FraudRLConfig) int {
	if cfg.BaseLimitPerMin <= 0 {
		cfg.BaseLimitPerMin = 100
	}
	pct := 100
	switch tier {
	case FraudRLTierSuspect:
		pct = cfg.SuspectPct
		if pct == 0 {
			pct = 50
		}
	case FraudRLTierIVT:
		pct = cfg.IVTPct
		if pct == 0 {
			pct = 10
		}
	case FraudRLTierBlock:
		pct = cfg.BlockPct
	}
	if pct <= 0 {
		return 0
	}
	if pct >= 100 {
		return cfg.BaseLimitPerMin
	}
	limit := cfg.BaseLimitPerMin * pct / 100
	if limit < 1 {
		return 1
	}
	return limit
}

// RetryAfterSec returns Retry-After seconds for a tier.
func RetryAfterSec(tier FraudRLTier, cfg FraudRLConfig) int {
	switch tier {
	case FraudRLTierBlock:
		if cfg.RetryBlockSec > 0 {
			return cfg.RetryBlockSec
		}
		return 120
	case FraudRLTierIVT:
		if cfg.RetryIVTSec > 0 {
			return cfg.RetryIVTSec
		}
		return 60
	case FraudRLTierSuspect:
		if cfg.RetrySuspectSec > 0 {
			return cfg.RetrySuspectSec
		}
		return 30
	default:
		if cfg.RetrySuspectSec > 0 {
			return cfg.RetrySuspectSec
		}
		return 30
	}
}

// ShouldBlockTier reports whether the tier is an immediate edge block (score > 80).
func ShouldBlockTier(tier FraudRLTier) bool {
	return tier == FraudRLTierBlock
}

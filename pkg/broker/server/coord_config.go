package server

import "time"

// CoordConfig tunes Redis leader lease behavior for HA broker clusters.
type CoordConfig struct {
	LeaseTTL           time.Duration
	Interval           time.Duration
	RenewFailThreshold int
	DebounceWindow     time.Duration
}

// DefaultCoordConfig returns production-oriented lease defaults.
func DefaultCoordConfig() CoordConfig {
	return CoordConfig{
		LeaseTTL:           15 * time.Second,
		Interval:           3 * time.Second,
		RenewFailThreshold: 3,
		DebounceWindow:     2 * time.Second,
	}
}

func (c CoordConfig) normalized() CoordConfig {
	out := c
	if out.LeaseTTL <= 0 {
		out.LeaseTTL = DefaultCoordConfig().LeaseTTL
	}
	if out.Interval <= 0 {
		out.Interval = DefaultCoordConfig().Interval
	}
	if out.RenewFailThreshold <= 0 {
		out.RenewFailThreshold = DefaultCoordConfig().RenewFailThreshold
	}
	if out.DebounceWindow <= 0 {
		out.DebounceWindow = DefaultCoordConfig().DebounceWindow
	}
	return out
}

func leaderSinceKey(topic string) string {
	return "espx:topics:" + topic + ":leader_since"
}

func leaderLastWinnerKey(topic string) string {
	return "espx:topics:" + topic + ":leader_last_winner"
}

package management

import (
	"time"

	"espx/internal/config"

	"github.com/jackc/pgx/v5/pgtype"
)

type blacklistTTLConfig struct {
	AutoTTLHours  int
	FraudTTLHours int
}

func blacklistTTLFromConfig(cfg *config.Config) blacklistTTLConfig {
	out := blacklistTTLConfig{
		AutoTTLHours:  24,
		FraudTTLHours: 168,
	}
	if cfg == nil {
		return out
	}
	if cfg.Management.BlacklistAutoTTLHours > 0 {
		out.AutoTTLHours = cfg.Management.BlacklistAutoTTLHours
	}
	if cfg.Management.BlacklistFraudTTLHours > 0 {
		out.FraudTTLHours = cfg.Management.BlacklistFraudTTLHours
	}
	return out
}

// resolveBlacklistExpiry maps reason and optional TTL override to a Postgres expiry timestamp.
// manual blocks are permanent unless ttl_seconds is set explicitly.
func resolveBlacklistExpiry(reason string, ttlSeconds *int64, cfg blacklistTTLConfig) pgtype.Timestamptz {
	if ttlSeconds != nil {
		if *ttlSeconds <= 0 {
			return pgtype.Timestamptz{}
		}
		return pgtype.Timestamptz{
			Time:  time.Now().UTC().Add(time.Duration(*ttlSeconds) * time.Second),
			Valid: true,
		}
	}

	reason = normalizeBlacklistReason(reason)
	switch reason {
	case "manual":
		return pgtype.Timestamptz{}
	case "auto":
		if cfg.AutoTTLHours <= 0 {
			return pgtype.Timestamptz{}
		}
		return pgtype.Timestamptz{
			Time:  time.Now().UTC().Add(time.Duration(cfg.AutoTTLHours) * time.Hour),
			Valid: true,
		}
	case "fraud":
		if cfg.FraudTTLHours <= 0 {
			return pgtype.Timestamptz{}
		}
		return pgtype.Timestamptz{
			Time:  time.Now().UTC().Add(time.Duration(cfg.FraudTTLHours) * time.Hour),
			Valid: true,
		}
	default:
		return pgtype.Timestamptz{}
	}
}

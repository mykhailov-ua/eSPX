package management

import (
	"testing"
	"time"
)

func TestBlacklistJanitor_DefaultInterval(t *testing.T) {
	j := NewBlacklistJanitor(nil, 0)
	if j == nil {
		t.Fatal("expected janitor")
	}
	if j.interval != time.Minute {
		t.Fatalf("interval: got %v want 1m", j.interval)
	}
}

func TestResolveBlacklistExpiry_ManualPermanent(t *testing.T) {
	cfg := defaultBlacklistTTLConfig()
	expires := resolveBlacklistExpiry("manual", nil, cfg)
	if expires.Valid {
		t.Fatal("manual blocks should not expire by default")
	}
}

func TestResolveBlacklistExpiry_FraudDefault(t *testing.T) {
	cfg := defaultBlacklistTTLConfig()
	cfg.FraudTTLHours = 168
	expires := resolveBlacklistExpiry("fraud", nil, cfg)
	if !expires.Valid {
		t.Fatal("expected fraud TTL")
	}
	if time.Until(expires.Time) < 167*time.Hour {
		t.Fatalf("fraud TTL too short: %v", expires.Time)
	}
}

func TestResolveBlacklistExpiry_ExplicitSeconds(t *testing.T) {
	cfg := defaultBlacklistTTLConfig()
	ttl := int64(3600)
	expires := resolveBlacklistExpiry("auto", &ttl, cfg)
	if !expires.Valid {
		t.Fatal("expected explicit TTL")
	}
	if time.Until(expires.Time) < 59*time.Minute {
		t.Fatalf("explicit TTL too short: %v", expires.Time)
	}
}

func TestResolveBlacklistExpiry_ZeroTTLPermanent(t *testing.T) {
	cfg := defaultBlacklistTTLConfig()
	zero := int64(0)
	expires := resolveBlacklistExpiry("auto", &zero, cfg)
	if expires.Valid {
		t.Fatal("zero TTL should mean permanent")
	}
}

func defaultBlacklistTTLConfig() blacklistTTLConfig {
	return blacklistTTLConfig{
		AutoTTLHours:  24,
		FraudTTLHours: 168,
	}
}

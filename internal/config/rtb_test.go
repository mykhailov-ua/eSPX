package config

import "testing"

func TestRtbModeParsing(t *testing.T) {
	assertMode := func(raw string, want RtbMode) {
		t.Helper()
		if got := ParseRtbMode(raw); got != want {
			t.Fatalf("ParseRtbMode(%q) = %q, want %q", raw, got, want)
		}
	}
	assertMode("", RtbModeOff)
	assertMode("off", RtbModeOff)
	assertMode("shadow", RtbModeShadow)
	assertMode("live", RtbModeLive)
}

func TestRtbBudgetAuthoritative(t *testing.T) {
	cfg := &Config{RtbMode: "live", RtbBudgetAuthority: "rtb"}
	if !cfg.RtbBudgetAuthoritative() {
		t.Fatal("expected rtb budget authority")
	}
	cfg.RtbBudgetAuthority = "redis"
	if cfg.RtbBudgetAuthoritative() {
		t.Fatal("redis authority must not skip lua budget")
	}
	cfg.RtbMode = "shadow"
	if cfg.RtbBudgetAuthoritative() {
		t.Fatal("shadow must not be budget authoritative")
	}
}

func TestRtbTargetingIndexDefaultOn(t *testing.T) {
	// env.go defaults RTB_TARGETING_INDEX=true; zero Config is false until loaded.
	cfg := &Config{RtbTargetingIndex: true}
	if !cfg.RtbTargetingIndexEnabled() {
		t.Fatal("targeting index must default on after env load")
	}
	cfg.RtbTargetingIndex = false
	if cfg.RtbTargetingIndexEnabled() {
		t.Fatal("expected targeting index disabled when explicitly off")
	}
}

package config

import (
	"strings"
	"testing"
)

// Guards production rejects FILTER_TIMEOUT_MS above the tracker SLA ceiling.
func TestLoad_productionFilterTimeoutCeiling(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("SERVER_PORT", "8181")
	t.Setenv("DB_DSN", "postgres://u:p@localhost/db?sslmode=disable")
	t.Setenv("REDIS_ADDRS", "127.0.0.1:6479,127.0.0.1:6480,127.0.0.1:6481,127.0.0.1:6482")
	t.Setenv("TOKEN_SYMMETRIC_KEY", "01234567890123456789012345678901")
	t.Setenv("FILTER_TIMEOUT_MS", "101")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "FILTER_TIMEOUT_MS") {
		t.Fatalf("expected FILTER_TIMEOUT_MS error, got %v", err)
	}

	t.Setenv("FILTER_TIMEOUT_MS", "100")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FilterTimeoutMs != 100 {
		t.Fatalf("FilterTimeoutMs=%d want 100", cfg.FilterTimeoutMs)
	}
}

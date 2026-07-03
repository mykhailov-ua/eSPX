package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards required ivt-detector environment variables are validated at startup.
func TestLoadIVTDetector_requiresSecrets(t *testing.T) {
	t.Setenv("DB_DSN", "")
	t.Setenv("CH_DSN", "")
	t.Setenv("ADMIN_API_KEY", "")

	_, err := LoadIVTDetector()
	require.Error(t, err)
}

// Guards management URL falls back to MANAGEMENT_PORT when MANAGEMENT_URL is unset.
func TestLoadIVTDetector_managementURLFallback(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/db")
	t.Setenv("CH_DSN", "clickhouse://localhost:9000/ad_event_processor")
	t.Setenv("ADMIN_API_KEY", "secret")
	t.Setenv("MANAGEMENT_URL", "")
	t.Setenv("MANAGEMENT_PORT", "9191")

	cfg, err := LoadIVTDetector()
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:9191", cfg.ManagementURL)
}

// Guards env defaults match production-oriented detector tuning.
func TestLoadIVTDetector_defaults(t *testing.T) {
	_ = os.Unsetenv("IVT_DETECTOR_SCAN_INTERVAL_MS")
	t.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/db")
	t.Setenv("CH_DSN", "clickhouse://localhost:9000/ad_event_processor")
	t.Setenv("ADMIN_API_KEY", "secret")

	cfg, err := LoadIVTDetector()
	require.NoError(t, err)
	assert.Equal(t, 300000, cfg.ScanIntervalMs)
	assert.Equal(t, int64(500), cfg.OutboxPendingLimit)
	assert.InDelta(t, 5.0, cfg.ClickToImpRatio, 0.001)
}

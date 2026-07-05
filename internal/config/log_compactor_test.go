package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadLogCompactor_defaults(t *testing.T) {
	t.Setenv("LOG_COMPACTOR_BACKEND", "local")
	t.Setenv("LOG_COMPACTOR_SOURCE_DIR", "/tmp/logs")
	t.Setenv("LOG_COMPACTOR_WARM_DIR", "/tmp/warm")

	cfg, err := LoadLogCompactor()
	require.NoError(t, err)
	assert.Equal(t, "local", cfg.Backend)
	assert.Equal(t, "/tmp/logs", cfg.SourceDir)
	assert.Equal(t, "/tmp/warm", cfg.WarmDir)
	assert.Equal(t, 1000, cfg.SampleRate)
}

func TestLoadLogCompactor_invalidBackend(t *testing.T) {
	t.Setenv("LOG_COMPACTOR_BACKEND", "gcs")
	_, err := LoadLogCompactor()
	require.Error(t, err)
}

func TestLoadLogCompactor_s3BackendAllowed(t *testing.T) {
	t.Setenv("LOG_COMPACTOR_BACKEND", "s3")
	t.Setenv("LOG_COMPACTOR_S3_BUCKET", "audit")
	t.Setenv("LOG_COMPACTOR_S3_REGION", "eu-west-1")

	cfg, err := LoadLogCompactor()
	require.NoError(t, err)
	assert.Equal(t, "s3", cfg.Backend)
}

func TestLoadLogCompactor_coldRequiresCHDSN(t *testing.T) {
	t.Setenv("LOG_COMPACTOR_BACKEND", "local")
	t.Setenv("LOG_COMPACTOR_COLD_ENABLED", "true")
	t.Setenv("CH_DSN", "")

	_, err := LoadLogCompactor()
	require.Error(t, err)
}

package ivtdetector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTLSImpersonating(t *testing.T) {
	assert.True(t, IsTLSImpersonating(
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
		"37b37375c33a2e6a17b2b6400c436321",
	))
	assert.False(t, IsTLSImpersonating(
		"Mozilla/5.0 Chrome/120.0.0.0",
		"chrome-ja3-fingerprint",
	))
}

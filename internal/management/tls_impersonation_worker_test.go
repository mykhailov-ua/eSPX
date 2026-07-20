package management

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsImpersonating(t *testing.T) {
	// Chrome UA + python-requests JA3 -> impersonation detected
	assert.True(t, IsImpersonating(
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"37b37375c33a2e6a17b2b6400c436321",
	))

	// Chrome UA + python-requests in JA3 -> impersonation detected
	assert.True(t, IsImpersonating(
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"python-requests-ja3-fingerprint",
	))

	// Normal Chrome UA + normal Chrome JA3 -> no impersonation
	assert.False(t, IsImpersonating(
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"chrome-ja3-fingerprint",
	))

	// Normal Python UA + python-requests JA3 -> no impersonation (not pretending to be Chrome)
	assert.False(t, IsImpersonating(
		"python-requests/2.31.0",
		"37b37375c33a2e6a17b2b6400c436321",
	))
}

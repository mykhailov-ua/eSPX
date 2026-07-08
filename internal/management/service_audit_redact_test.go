package management

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedactJSONPII_masksEmailAndIP(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"email":"ops@example.com","ip":"203.0.113.10","nested":{"ip_address":"10.0.0.1"}}`)
	out := redactJSONPII(raw)
	assert.Contains(t, string(out), "[REDACTED_EMAIL]")
	assert.Contains(t, string(out), "[REDACTED_IP]")
	assert.NotContains(t, string(out), "ops@example.com")
	assert.NotContains(t, string(out), "203.0.113.10")
}

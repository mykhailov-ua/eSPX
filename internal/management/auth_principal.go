package management

import (
	"github.com/google/uuid"
)

// adminAPIKeyNamespace anchors deterministic principals derived from shared admin API keys.
var adminAPIKeyNamespace = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

// apiKeyPrincipalID returns a stable UUID for audit attribution of automation callers.
func apiKeyPrincipalID(apiKey string) uuid.UUID {
	return uuid.NewSHA1(adminAPIKeyNamespace, []byte(apiKey))
}

package payment

import "errors"

// ErrProviderNotConfigured signals that credentials exist but live checkout is not wired,
// so callers fail fast instead of creating intents that cannot be paid.
var ErrProviderNotConfigured = errors.New("stripe provider not configured: wire checkout session in provider_stripe.go")

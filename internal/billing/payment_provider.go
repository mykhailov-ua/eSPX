package billing

import "log/slog"

// PaymentProvider names the external checkout integration for invoice UX copy.
type PaymentProvider interface {
	Name() string
	Configured() bool
}

// PlaceholderProvider is the M2.8 default; never performs outbound HTTP.
type PlaceholderProvider struct {
	name string
	key  string
}

// NewPaymentProvider returns the configured billing payment provider.
func NewPaymentProvider(providerName, providerKey string) PaymentProvider {
	if providerName == "" {
		providerName = "placeholder"
	}
	p := &PlaceholderProvider{name: providerName, key: providerKey}
	if providerName == "placeholder" {
		slog.Info("billing payment provider: placeholder mode", "key_set", providerKey != "")
	}
	return p
}

// Name implements PaymentProvider.
func (p *PlaceholderProvider) Name() string {
	if p == nil || p.name == "" {
		return "placeholder"
	}
	return p.name
}

// Configured implements PaymentProvider.
func (p *PlaceholderProvider) Configured() bool {
	return false
}

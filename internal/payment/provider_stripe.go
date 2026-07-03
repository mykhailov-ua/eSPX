package payment

import (
	"context"
	"fmt"

	"espx/internal/config"
)

// StripeProvider holds Stripe API credentials. Checkout session creation is wired in Phase 2 (stripe-go).
type StripeProvider struct {
	secretKey string
}

// NewStripeProvider holds credentials until stripe-go checkout wiring lands in createStripeCheckoutSession.
func NewStripeProvider(cfg *config.Config) *StripeProvider {
	return &StripeProvider{secretKey: string(cfg.StripeSecretKey)}
}

// Name matches webhook and intent rows so Stripe events resolve by provider_ref.
func (p *StripeProvider) Name() string {
	return "stripe"
}

// CreateCheckout rejects misaligned amounts before any provider call because Stripe cannot settle sub-cent values.
func (p *StripeProvider) CreateCheckout(ctx context.Context, amountMicro int64, currency string, metadata map[string]string, idempotencyKey string) (string, string, error) {
	if p.secretKey == "" {
		return "", "", ErrProviderNotConfigured
	}
	if _, err := MicroToStripeAmount(amountMicro); err != nil {
		return "", "", fmt.Errorf("stripe checkout amount: %w", err)
	}
	_ = ctx
	_ = currency
	_ = metadata
	_ = idempotencyKey
	return createStripeCheckoutSession(p.secretKey, amountMicro, currency, metadata, idempotencyKey)
}

// createStripeCheckoutSession is the single stripe-go integration point so checkout wiring stays isolated.
func createStripeCheckoutSession(secretKey string, amountMicro int64, currency string, metadata map[string]string, idempotencyKey string) (providerRef string, checkoutURL string, err error) {
	_ = secretKey
	_ = amountMicro
	_ = currency
	_ = metadata
	_ = idempotencyKey
	return "", "", ErrProviderNotConfigured
}

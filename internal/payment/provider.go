package payment

import (
	"context"
	"log/slog"

	"espx/internal/config"
)

// ParsedEvent is a normalized provider webhook payload (reserved for live Stripe).
type ParsedEvent struct {
	EventID         string
	EventType       string
	PaymentIntentID string
	AmountMicro     int64
	ProviderRef     string
}

// Provider abstracts checkout session creation and future webhook parsing.
type Provider interface {
	Name() string
	CreateCheckout(ctx context.Context, amountMicro int64, currency string, metadata map[string]string, idempotencyKey string) (providerRef string, checkoutURL string, err error)
}

// StripeConfigured selects live Stripe mode only when a secret key is present, avoiding accidental mock checkout in prod.
func StripeConfigured(cfg *config.Config) bool {
	return cfg != nil && string(cfg.StripeSecretKey) != ""
}

// NewProvider picks mock or Stripe at startup so local stacks work without credentials.
func NewProvider(cfg *config.Config) Provider {
	if StripeConfigured(cfg) {
		return NewStripeProvider(cfg)
	}
	return NewMockProvider()
}

// LogProviderMode surfaces misconfiguration at boot because payment failures are hard to trace from UI alone.
func LogProviderMode(cfg *config.Config) {
	if StripeConfigured(cfg) {
		slog.Info("payment provider mode", "provider", "stripe", "checkout_api", "stripe_go")
		if string(cfg.StripeWebhookSecret) == "" {
			slog.Warn("STRIPE_WEBHOOK_SECRET unset; POST /webhooks/stripe returns 503")
		}
		if string(cfg.PaymentInternalToken) == "" {
			slog.Warn("PAYMENT_INTERNAL_TOKEN unset; gRPC CreatePaymentIntent rejects callers")
		}
		return
	}
	slog.Info("payment provider mode", "provider", "mock")
}

// MockProvider supplies deterministic checkout refs for local and integration testing.
type MockProvider struct{}

// NewMockProvider enables end-to-end payment flows without Stripe credentials or network calls.
func NewMockProvider() *MockProvider {
	return &MockProvider{}
}

// Name stays "stripe" on mock intents so webhook routing and provider_ref lookups stay uniform in tests.
func (mockProvider *MockProvider) Name() string {
	return "stripe"
}

// CreateCheckout derives provider_ref from the idempotency key so mock webhooks can target the right intent.
func (mockProvider *MockProvider) CreateCheckout(ctx context.Context, amountMicro int64, currency string, metadata map[string]string, idempotencyKey string) (string, string, error) {
	return "pi_mock_" + idempotencyKey, "https://checkout.stripe.dev/pay/mock_" + idempotencyKey, nil
}

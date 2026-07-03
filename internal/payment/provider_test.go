package payment

import (
	"testing"

	"espx/internal/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProvider_mockByDefault(t *testing.T) {
	p := NewProvider(&config.Config{})
	_, ok := p.(*MockProvider)
	assert.True(t, ok)
}

func TestNewProvider_stripeWhenKeySet(t *testing.T) {
	p := NewProvider(&config.Config{StripeSecretKey: "sk_test_x"})
	_, ok := p.(*StripeProvider)
	assert.True(t, ok)
}

func TestStripeProvider_CreateCheckout_unalignedAmount(t *testing.T) {
	p := NewStripeProvider(&config.Config{StripeSecretKey: "sk_test_x"})
	_, _, err := p.CreateCheckout(t.Context(), 10_001, "USD", nil, "idem-1")
	require.Error(t, err)
}

func TestStripeProvider_CreateCheckout_notWired(t *testing.T) {
	p := NewStripeProvider(&config.Config{StripeSecretKey: "sk_test_x"})
	_, _, err := p.CreateCheckout(t.Context(), 10_000_000, "USD", nil, "idem-1")
	require.ErrorIs(t, err, ErrProviderNotConfigured)
}

func TestMergeIntentMetadata_checkoutURL(t *testing.T) {
	raw, err := mergeIntentMetadata(map[string]string{"foo": "bar"}, "https://checkout.example/pay")
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"checkout_url":"https://checkout.example/pay"`)
	assert.Contains(t, string(raw), `"foo":"bar"`)
}

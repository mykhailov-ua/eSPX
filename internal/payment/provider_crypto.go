package payment

import (
	"context"
	"fmt"
)

// CryptoProvider implements crypto acceptance for USDT (TRC20/ERC20) with confirmation depth checks.
type CryptoProvider struct {
	confirmationDepth int
	minPaymentMicro   int64
	webhookSecret     string
}

// NewCryptoProvider instantiates a CryptoProvider with confirmation depth and min payment limits.
func NewCryptoProvider(confirmationDepth int, minPaymentMicro int64, webhookSecret string) *CryptoProvider {
	if confirmationDepth <= 0 {
		confirmationDepth = 12 // default 12 blocks for ERC20 USDT
	}
	return &CryptoProvider{
		confirmationDepth: confirmationDepth,
		minPaymentMicro:   minPaymentMicro,
		webhookSecret:     webhookSecret,
	}
}

// Name returns the provider identifier.
func (p *CryptoProvider) Name() string {
	return "crypto"
}

// CreateCheckout generates a mock checkout URL and provider reference for crypto payments.
func (p *CryptoProvider) CreateCheckout(ctx context.Context, amountMicro int64, currency string, metadata map[string]string, idempotencyKey string) (string, string, error) {
	if amountMicro < p.minPaymentMicro {
		return "", "", fmt.Errorf("amount %d micro is below minimum payment %d micro", amountMicro, p.minPaymentMicro)
	}
	_ = ctx
	providerRef := "tx_crypto_" + idempotencyKey
	checkoutURL := "https://checkout.crypto.dev/pay/" + idempotencyKey
	return providerRef, checkoutURL, nil
}

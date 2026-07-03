package payment

import (
	"context"
	"sync/atomic"
)

// CountingMockProvider counts CreateCheckout calls for concurrency chaos tests.
type CountingMockProvider struct {
	calls atomic.Int32
}

// NewCountingMockProvider wraps the mock provider to assert checkout is not called more than once under chaos load.
func NewCountingMockProvider() *CountingMockProvider {
	return &CountingMockProvider{}
}

// Calls exposes checkout invocation count because chaos tests assert idempotency under concurrency.
func (m *CountingMockProvider) Calls() int {
	return int(m.calls.Load())
}

// Name matches MockProvider so webhook routing stays consistent under concurrency tests.
func (m *CountingMockProvider) Name() string {
	return "stripe"
}

// CreateCheckout increments the counter before returning deterministic refs for webhook correlation tests.
func (m *CountingMockProvider) CreateCheckout(ctx context.Context, amountMicro int64, currency string, metadata map[string]string, idempotencyKey string) (string, string, error) {
	m.calls.Add(1)
	return "pi_mock_" + idempotencyKey, "https://checkout.stripe.dev/pay/mock_" + idempotencyKey, nil
}

package payment

import (
	"context"
	"fmt"
	"testing"

	"espx/internal/config"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessStripeDisputeWebhook_noDoubleChargeback proves duplicate funds_withdrawn events do not enqueue a second outbox row.
func TestProcessStripeDisputeWebhook_noDoubleChargeback(t *testing.T) {
	if testing.Short() {
		t.Skip("requires testcontainers")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	cfg := &config.Config{MaxRetries: 3}
	svc := NewService(pool, NewMockProvider(), cfg)
	ctx := context.Background()

	customerID := uuid.New()
	amountMicro := int64(16_000_000)
	result, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USD", "dispute-double-"+uuid.New().String(), nil)
	require.NoError(t, err)
	providerRef := result.Intent.ProviderRef.String

	stripeCents, err := MicroToStripeAmount(amountMicro)
	require.NoError(t, err)
	successPayload := fmt.Sprintf(`{"id":"evt_topup","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":%d}}}`, providerRef, stripeCents)
	err = svc.ProcessStripeWebhook(ctx, "evt_topup", "payment_intent.succeeded", []byte(successPayload), providerRef, amountMicro, successPayload)
	require.NoError(t, err)

	disputeID := "dp_double_" + uuid.New().String()
	withdrawMicro := int64(6_000_000)
	withdrawCents, err := MicroToStripeAmount(withdrawMicro)
	require.NoError(t, err)

	mkPayload := func(eventID string) string {
		return fmt.Sprintf(`{"id":"%s","type":"charge.dispute.funds_withdrawn","data":{"object":{"id":"%s","amount":%d,"payment_intent":"%s","status":"needs_response"}}}`,
			eventID, disputeID, withdrawCents, providerRef)
	}

	err = svc.ProcessStripeDisputeWebhook(ctx, "evt_dp_a", "charge.dispute.funds_withdrawn", []byte(mkPayload("evt_dp_a")), disputeID, providerRef, withdrawMicro, "needs_response")
	require.NoError(t, err)

	err = svc.ProcessStripeDisputeWebhook(ctx, "evt_dp_b", "charge.dispute.funds_withdrawn", []byte(mkPayload("evt_dp_b")), disputeID, providerRef, withdrawMicro, "needs_response")
	require.NoError(t, err)

	var chargebackCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_outbox WHERE event_type = $1`, OutboxEventApplyChargeback).Scan(&chargebackCount))
	assert.Equal(t, 1, chargebackCount, "duplicate dispute withdrawal must not enqueue second chargeback")
}

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

// TestProcessStripeRefundWebhook_noDoubleDebit proves a second refund event id does not enqueue another outbox row for the same refund id.
func TestProcessStripeRefundWebhook_noDoubleDebit(t *testing.T) {
	if testing.Short() {
		t.Skip("requires testcontainers")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	cfg := &config.Config{MaxRetries: 3}
	svc := NewService(pool, NewMockProvider(), cfg)
	ctx := context.Background()

	customerID := uuid.New()
	amountMicro := int64(20_000_000)
	result, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USD", "refund-double-"+uuid.New().String(), nil)
	require.NoError(t, err)
	providerRef := result.Intent.ProviderRef.String

	stripeCents, err := MicroToStripeAmount(amountMicro)
	require.NoError(t, err)
	successPayload := fmt.Sprintf(`{"id":"evt_topup","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":%d}}}`, providerRef, stripeCents)
	err = svc.ProcessStripeWebhook(ctx, "evt_topup", "payment_intent.succeeded", []byte(successPayload), providerRef, amountMicro, successPayload)
	require.NoError(t, err)

	refundID := "re_double_" + uuid.New().String()
	refundMicro := int64(7_000_000)
	refundCents, err := MicroToStripeAmount(refundMicro)
	require.NoError(t, err)

	mkRefundPayload := func(eventID string) string {
		return fmt.Sprintf(`{"id":"%s","type":"refund.created","data":{"object":{"id":"%s","amount":%d,"payment_intent":"%s","status":"succeeded"}}}`,
			eventID, refundID, refundCents, providerRef)
	}

	err = svc.ProcessStripeRefundWebhook(ctx, "evt_ref_a", "refund.created", []byte(mkRefundPayload("evt_ref_a")), refundID, providerRef, refundMicro, "succeeded")
	require.NoError(t, err)

	err = svc.ProcessStripeRefundWebhook(ctx, "evt_ref_b", "refund.created", []byte(mkRefundPayload("evt_ref_b")), refundID, providerRef, refundMicro, "succeeded")
	require.NoError(t, err)

	var reverseCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_outbox WHERE event_type = $1`, OutboxEventReverseBalance).Scan(&reverseCount))
	assert.Equal(t, 1, reverseCount, "duplicate refund id must not enqueue second reverse balance")
}

package payment

import (
	"context"
	"fmt"
	"testing"

	"espx/internal/config"
	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_StripeCheckoutSettlement proves webhook replay is idempotent and does not double-credit balance.
func TestChaos_StripeCheckoutSettlement(t *testing.T) {
	if testing.Short() {
		t.Skip("requires testcontainers")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	cfg := &config.Config{MaxRetries: 3}
	svc := NewService(pool, NewMockProvider(), cfg)
	ctx := context.Background()

	customerID := uuid.New()
	amountMicro := int64(9_500_000)
	result, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USD", "stripe-chaos-"+uuid.New().String(), nil)
	require.NoError(t, err)
	providerRef := result.Intent.ProviderRef.String

	stripeCents, err := MicroToStripeAmount(amountMicro)
	require.NoError(t, err)
	eventID := "evt_chaos_settle_" + uuid.New().String()
	payload := fmt.Sprintf(`{"id":"%s","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":%d}}}`, eventID, providerRef, stripeCents)

	err = svc.ProcessStripeWebhook(ctx, eventID, "payment_intent.succeeded", []byte(payload), providerRef, amountMicro, payload)
	require.NoError(t, err)

	status, err := svc.ReplayWebhook(ctx, "stripe", eventID)
	require.NoError(t, err)
	assert.Equal(t, "already_processed", status)

	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_outbox WHERE event_type = 'SETTLE_BALANCE'`).Scan(&outboxCount))
	assert.Equal(t, 1, outboxCount, "replay must not enqueue a second settlement outbox row")

	var webhookStatus db.PaymentWebhookEventStatus
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT status FROM payment.webhook_events WHERE provider = 'stripe' AND provider_event_id = $1`, eventID).Scan(&webhookStatus))
	assert.Equal(t, db.PaymentWebhookEventStatusPROCESSED, webhookStatus)
}

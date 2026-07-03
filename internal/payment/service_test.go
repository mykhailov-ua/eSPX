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

// TestProcessStripeWebhook_noDoubleSettlement proves a second success event does not enqueue another outbox row.
func TestProcessStripeWebhook_noDoubleSettlement(t *testing.T) {
	if testing.Short() {
		t.Skip("requires testcontainers")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	cfg := &config.Config{MaxRetries: 3}
	svc := NewService(pool, NewMockProvider(), cfg)
	ctx := context.Background()

	customerID := uuid.New()
	idempotencyKey := "double-settle-" + uuid.New().String()
	amountMicro := int64(10_000_000)

	result, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USD", idempotencyKey, nil)
	require.NoError(t, err)
	intent := result.Intent
	providerRef := intent.ProviderRef.String

	payload1 := fmt.Sprintf(`{"id":"evt_a","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":%d}}}`, providerRef, amountMicro)
	err = svc.ProcessStripeWebhook(ctx, "evt_a", "payment_intent.succeeded", []byte(payload1), providerRef, amountMicro, payload1)
	require.NoError(t, err)

	payload2 := fmt.Sprintf(`{"id":"evt_b","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":%d}}}`, providerRef, amountMicro)
	err = svc.ProcessStripeWebhook(ctx, "evt_b", "payment_intent.succeeded", []byte(payload2), providerRef, amountMicro, payload2)
	require.NoError(t, err)

	q := db.New(pool)
	outbox, err := q.GetPendingOutboxEventsForUpdate(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, outbox, 1, "second success webhook must not enqueue duplicate settlement")

	updated, err := svc.GetPaymentIntent(ctx, uuid.UUID(intent.ID.Bytes))
	require.NoError(t, err)
	assert.Equal(t, db.PaymentPaymentIntentStatusSUCCEEDED, updated.Status)
}

// TestProcessStripeWebhook_zeroAmountRejected ignores success webhooks with non-positive amounts.
func TestProcessStripeWebhook_zeroAmountRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("requires testcontainers")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	cfg := &config.Config{}
	svc := NewService(pool, NewMockProvider(), cfg)
	ctx := context.Background()

	customerID := uuid.New()
	result, err := svc.CreatePaymentIntent(ctx, customerID, 5_000_000, "USD", "zero-amt-"+uuid.New().String(), nil)
	require.NoError(t, err)
	intent := result.Intent
	providerRef := intent.ProviderRef.String
	payload := fmt.Sprintf(`{"id":"evt_z","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":0}}}`, providerRef)
	err = svc.ProcessStripeWebhook(ctx, "evt_z", "payment_intent.succeeded", []byte(payload), providerRef, 0, payload)
	require.NoError(t, err)

	q := db.New(pool)
	outbox, err := q.GetPendingOutboxEventsForUpdate(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, outbox)

	wh, err := q.GetWebhookEvent(ctx, db.GetWebhookEventParams{Provider: "stripe", ProviderEventID: "evt_z"})
	require.NoError(t, err)
	assert.Equal(t, db.PaymentWebhookEventStatusIGNORED, wh.Status)
}

func TestProcessStripeWebhook_amountMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("requires testcontainers")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	cfg := &config.Config{}
	svc := NewService(pool, NewMockProvider(), cfg)
	ctx := context.Background()

	customerID := uuid.New()
	result, err := svc.CreatePaymentIntent(ctx, customerID, 5_000_000, "USD", "amt-mismatch-"+uuid.New().String(), nil)
	require.NoError(t, err)
	intent := result.Intent

	providerRef := intent.ProviderRef.String
	payload := fmt.Sprintf(`{"id":"evt_m","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":999}}}`, providerRef)
	err = svc.ProcessStripeWebhook(ctx, "evt_m", "payment_intent.succeeded", []byte(payload), providerRef, 999, payload)
	require.NoError(t, err)

	q := db.New(pool)
	outbox, err := q.GetPendingOutboxEventsForUpdate(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, outbox)

	wh, err := q.GetWebhookEvent(ctx, db.GetWebhookEventParams{Provider: "stripe", ProviderEventID: "evt_m"})
	require.NoError(t, err)
	assert.Equal(t, db.PaymentWebhookEventStatusIGNORED, wh.Status)
}

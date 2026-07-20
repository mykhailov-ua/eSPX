package payment

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestChaos_CryptoWebhookStormIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()

	infra.Cfg.CryptoWebhookSecret = "crypto_test_secret"
	infra.Cfg.CryptoMinPaymentMicro = 10_000_000
	infra.Cfg.CryptoConfirmationDepth = 12

	svc := NewService(infra.Pool, NewProvider(infra.Cfg), infra.Cfg)

	customerID := uuid.New()
	_, err := infra.Pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency)
		VALUES ($1, 'Crypto Customer', 0.00, 'USD')
	`, customerID)
	require.NoError(t, err)

	idempotencyKey := "crypto-storm-" + uuid.New().String()
	amountMicro := int64(50_000_000)

	res, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USDT", idempotencyKey, map[string]string{
		"provider": "crypto",
	})
	require.NoError(t, err)

	eventID := "evt_crypto_storm_" + uuid.New().String()
	evtPayload := cryptoEvent{
		ID:            eventID,
		Type:          "payment.succeeded",
		TxHash:        "0xabc123",
		AmountMicro:   amountMicro,
		Currency:      "USDT",
		Confirmations: 12,
		ProviderRef:   res.Intent.ProviderRef.String,
	}
	bodyBytes, err := json.Marshal(evtPayload)
	require.NoError(t, err)

	// Simulate a storm of concurrent duplicate webhooks
	const stormSize = 50
	var wg sync.WaitGroup
	wg.Add(stormSize)

	for i := 0; i < stormSize; i++ {
		go func() {
			defer wg.Done()
			// Each goroutine attempts to process the exact same webhook payload concurrently
			_ = svc.ProcessCryptoWebhook(ctx, eventID, "payment.succeeded", bodyBytes, res.Intent.ProviderRef.String, amountMicro, "0xabc123", 12)
		}()
	}
	wg.Wait()

	// Verify intent is SUCCEEDED
	intent, err := svc.GetPaymentIntent(ctx, uuid.UUID(res.Intent.ID.Bytes))
	require.NoError(t, err)
	require.Equal(t, db.PaymentPaymentIntentStatusSUCCEEDED, intent.Status)

	// Verify exactly ONE hold has been created for this intent
	var holdCount int
	err = infra.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.crypto_holds WHERE payment_intent_id = $1
	`, intent.ID).Scan(&holdCount)
	require.NoError(t, err)
	require.Equal(t, 1, holdCount, "exactly one hold must be created despite the webhook storm")

	// Emit chaos proof
	logChaosProof(t, "crypto_webhook_storm", map[string]string{"idempotent": "true"})
}

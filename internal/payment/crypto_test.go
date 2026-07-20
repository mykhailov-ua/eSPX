package payment

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCryptoGateway_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()

	// Configure crypto settings
	infra.Cfg.CryptoWebhookSecret = "crypto_test_secret"
	infra.Cfg.CryptoMinPaymentMicro = 10_000_000 // 10 USD
	infra.Cfg.CryptoConfirmationDepth = 12

	// Re-initialize service with updated config to pick up the crypto provider
	svc := NewService(infra.Pool, NewProvider(infra.Cfg), infra.Cfg)

	customerID := uuid.New()

	// Seed customer in database
	_, err := infra.Pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency)
		VALUES ($1, 'Crypto Customer', 0.00, 'USD')
	`, customerID)
	require.NoError(t, err)

	idempotencyKey := "crypto-idemp-" + uuid.New().String()
	amountMicro := int64(50_000_000) // 50 USD

	// 1. Create Payment Intent
	res, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USDT", idempotencyKey, map[string]string{
		"provider": "crypto",
	})
	require.NoError(t, err)
	require.Equal(t, "crypto", res.Intent.Provider)
	require.Equal(t, db.PaymentPaymentIntentStatusPENDINGPROVIDER, res.Intent.Status)

	providerRef := res.Intent.ProviderRef.String
	require.NotEmpty(t, providerRef)

	// 2. Send Webhook with insufficient confirmations (e.g. 5 confirmations)
	eventID := "evt_crypto_" + uuid.New().String()
	evtPayload := cryptoEvent{
		ID:            eventID,
		Type:          "payment.succeeded",
		TxHash:        "0xabc123",
		AmountMicro:   amountMicro,
		Currency:      "USDT",
		Confirmations: 5,
		ProviderRef:   providerRef,
	}
	bodyBytes, err := json.Marshal(evtPayload)
	require.NoError(t, err)

	// Process webhook
	err = svc.ProcessCryptoWebhook(ctx, eventID, "payment.succeeded", bodyBytes, providerRef, amountMicro, "0xabc123", 5)
	require.NoError(t, err)

	// Verify intent is in PROCESSING status
	intent, err := svc.GetPaymentIntent(ctx, uuid.UUID(res.Intent.ID.Bytes))
	require.NoError(t, err)
	require.Equal(t, db.PaymentPaymentIntentStatusPROCESSING, intent.Status)

	// Verify no hold has been created yet
	var holdCount int
	err = infra.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM payment.crypto_holds WHERE payment_intent_id = $1`, intent.ID).Scan(&holdCount)
	require.NoError(t, err)
	require.Equal(t, 0, holdCount)

	// 3. Send Webhook with enough confirmations (e.g. 12 confirmations)
	eventID2 := "evt_crypto_" + uuid.New().String()
	evtPayload.ID = eventID2
	evtPayload.Confirmations = 12
	bodyBytes2, err := json.Marshal(evtPayload)
	require.NoError(t, err)

	err = svc.ProcessCryptoWebhook(ctx, eventID2, "payment.succeeded", bodyBytes2, providerRef, amountMicro, "0xabc123", 12)
	require.NoError(t, err)

	// Verify intent is SUCCEEDED
	intent, err = svc.GetPaymentIntent(ctx, uuid.UUID(res.Intent.ID.Bytes))
	require.NoError(t, err)
	require.Equal(t, db.PaymentPaymentIntentStatusSUCCEEDED, intent.Status)

	// Verify hold is created in HELD status
	var holdStatus string
	var holdReleaseAt time.Time
	err = infra.Pool.QueryRow(ctx, `
		SELECT status, release_at FROM payment.crypto_holds WHERE payment_intent_id = $1
	`, intent.ID).Scan(&holdStatus, &holdReleaseAt)
	require.NoError(t, err)
	require.Equal(t, "HELD", holdStatus)
	require.True(t, holdReleaseAt.After(time.Now()))

	// 4. Fast-forward hold release_at to the past
	_, err = infra.Pool.Exec(ctx, `
		UPDATE payment.crypto_holds SET release_at = now() - interval '1 second' WHERE payment_intent_id = $1
	`, intent.ID)
	require.NoError(t, err)

	// Run CryptoHoldWorker to release hold
	holdWorker := NewCryptoHoldWorker(infra.Pool, infra.Cfg)
	err = holdWorker.ProcessHolds(ctx)
	require.NoError(t, err)

	// Verify hold is RELEASED
	err = infra.Pool.QueryRow(ctx, `
		SELECT status FROM payment.crypto_holds WHERE payment_intent_id = $1
	`, intent.ID).Scan(&holdStatus)
	require.NoError(t, err)
	require.Equal(t, "RELEASED", holdStatus)

	// Verify outbox event is enqueued
	var outboxCount int
	err = infra.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_outbox WHERE event_type = 'SETTLE_BALANCE'
	`).Scan(&outboxCount)
	require.NoError(t, err)
	require.Equal(t, 1, outboxCount)

	// Run OutboxWorker to settle balance
	outboxWorker := newOutboxWorkerForChaos(infra)
	n, err := outboxWorker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Verify customer balance is credited
	var balance int64
	err = infra.Pool.QueryRow(ctx, `SELECT balance FROM customers WHERE id = $1`, customerID).Scan(&balance)
	require.NoError(t, err)
	require.Equal(t, int64(50_000_000), balance)
}

func TestCryptoGateway_UnderpayRejected(t *testing.T) {
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

	idempotencyKey := "crypto-underpay-" + uuid.New().String()
	amountMicro := int64(50_000_000)

	res, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USDT", idempotencyKey, map[string]string{
		"provider": "crypto",
	})
	require.NoError(t, err)

	// Send Webhook with underpay amount (e.g. 40 USD instead of 50 USD)
	eventID := "evt_crypto_" + uuid.New().String()
	evtPayload := cryptoEvent{
		ID:            eventID,
		Type:          "payment.succeeded",
		TxHash:        "0xabc123",
		AmountMicro:   40_000_000, // Underpay!
		Currency:      "USDT",
		Confirmations: 12,
		ProviderRef:   res.Intent.ProviderRef.String,
	}
	bodyBytes, err := json.Marshal(evtPayload)
	require.NoError(t, err)

	err = svc.ProcessCryptoWebhook(ctx, eventID, "payment.succeeded", bodyBytes, res.Intent.ProviderRef.String, 40_000_000, "0xabc123", 12)
	require.NoError(t, err)

	// Verify intent is marked FAILED
	intent, err := svc.GetPaymentIntent(ctx, uuid.UUID(res.Intent.ID.Bytes))
	require.NoError(t, err)
	require.Equal(t, db.PaymentPaymentIntentStatusFAILED, intent.Status)

	// Verify no hold has been created
	var holdCount int
	err = infra.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM payment.crypto_holds WHERE payment_intent_id = $1`, intent.ID).Scan(&holdCount)
	require.NoError(t, err)
	require.Equal(t, 0, holdCount)
}

func TestCryptoGateway_FraudGateBlocksRelease(t *testing.T) {
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

	idempotencyKey := "crypto-fraud-" + uuid.New().String()
	amountMicro := int64(50_000_000)

	res, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USDT", idempotencyKey, map[string]string{
		"provider": "crypto",
	})
	require.NoError(t, err)

	// Process successful webhook to create hold
	eventID := "evt_crypto_" + uuid.New().String()
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

	err = svc.ProcessCryptoWebhook(ctx, eventID, "payment.succeeded", bodyBytes, res.Intent.ProviderRef.String, amountMicro, "0xabc123", 12)
	require.NoError(t, err)

	// Fast-forward hold release_at
	_, err = infra.Pool.Exec(ctx, `
		UPDATE payment.crypto_holds SET release_at = now() - interval '1 second' WHERE payment_intent_id = $1
	`, res.Intent.ID)
	require.NoError(t, err)

	// Simulate fraud by inserting an active dispute for this customer
	disputeID := uuid.New()
	_, err = infra.Pool.Exec(ctx, `
		INSERT INTO payment.payment_disputes (id, payment_intent_id, provider, provider_dispute_id, amount_micro, status)
		VALUES ($1, $2, 'crypto', 'disp_crypto_123', $3, 'OPEN')
	`, disputeID, res.Intent.ID, amountMicro)
	require.NoError(t, err)

	// Run CryptoHoldWorker to release hold
	holdWorker := NewCryptoHoldWorker(infra.Pool, infra.Cfg)
	err = holdWorker.ProcessHolds(ctx)
	require.NoError(t, err)

	// Verify hold is marked FRAUD_BLOCKED
	var holdStatus string
	err = infra.Pool.QueryRow(ctx, `
		SELECT status FROM payment.crypto_holds WHERE payment_intent_id = $1
	`, res.Intent.ID).Scan(&holdStatus)
	require.NoError(t, err)
	require.Equal(t, "FRAUD_BLOCKED", holdStatus)

	// Verify no outbox event is enqueued
	var outboxCount int
	err = infra.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_outbox WHERE event_type = 'SETTLE_BALANCE'
	`).Scan(&outboxCount)
	require.NoError(t, err)
	require.Equal(t, 0, outboxCount)
}

func TestCryptoGateway_WebhookHTTPHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	infra.Cfg.CryptoWebhookSecret = "crypto_test_secret"
	infra.Cfg.CryptoMinPaymentMicro = 10_000_000
	infra.Cfg.CryptoConfirmationDepth = 12

	svc := NewService(infra.Pool, NewProvider(infra.Cfg), infra.Cfg)
	handler := NewWebhookHandler(svc, infra.Cfg)

	customerID := uuid.New()
	_, err := infra.Pool.Exec(context.Background(), `
		INSERT INTO customers (id, name, balance, currency)
		VALUES ($1, 'Crypto Customer', 0.00, 'USD')
	`, customerID)
	require.NoError(t, err)

	res, err := svc.CreatePaymentIntent(context.Background(), customerID, 50_000_000, "USDT", "crypto-http-idemp", map[string]string{
		"provider": "crypto",
	})
	require.NoError(t, err)

	eventID := "evt_crypto_http"
	evtPayload := cryptoEvent{
		ID:            eventID,
		Type:          "payment.succeeded",
		TxHash:        "0xabc123",
		AmountMicro:   50_000_000,
		Currency:      "USDT",
		Confirmations: 12,
		ProviderRef:   res.Intent.ProviderRef.String,
	}
	bodyBytes, err := json.Marshal(evtPayload)
	require.NoError(t, err)

	// Generate Stripe-style signature header
	ts := time.Now().Unix()
	tsStr := fmt.Sprintf("%d", ts)
	mac := hmac.New(sha256.New, []byte("crypto_test_secret"))
	mac.Write([]byte(tsStr + "." + string(bodyBytes)))
	sigHeader := fmt.Sprintf("t=%s,v1=%s", tsStr, hex.EncodeToString(mac.Sum(nil)))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/crypto", strings.NewReader(string(bodyBytes)))
	req.Header.Set("Crypto-Signature", sigHeader)

	rr := httptest.NewRecorder()
	handler.handleCryptoWebhook(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "OK", rr.Body.String())
}

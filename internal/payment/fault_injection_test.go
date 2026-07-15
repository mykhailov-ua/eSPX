package payment

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/ingestion"
	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const paymentChaosWorkers = 20

// TestChaos_PaymentDualOutboxWorkerRace guards concurrent workers credit exactly once per outbox event.
func TestChaos_PaymentDualOutboxWorkerRace(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 25_000_000, "chaos-race-"+uuid.New().String())
	assertPaymentChaosInvariants(t, infra.Pool, seed, 0, 0)

	worker := newOutboxWorkerForChaos(infra)
	const workers = 4
	var wg sync.WaitGroup
	var totalProcessed atomic.Int32
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			n, _ := worker.ProcessOutbox(ctx, 10)
			totalProcessed.Add(int32(n))
		}()
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return paymentOutboxStatus(t, infra.Pool, seed.OutboxID) == "PROCESSED" &&
			ledgerCountForIntent(t, infra.Pool, seed.IntentID) == 1
	}, 10*time.Second, 50*time.Millisecond)
	assertPaymentChaosInvariants(t, infra.Pool, seed, seed.AmountMicro, 1)

	logChaosProof(t, "outbox_worker_race", map[string]string{
		"subsystem":   "payment_outbox",
		"workers":     "4",
		"processed":   itoaPaymentChaos(int(totalProcessed.Load())),
		"ledger_rows": "1",
		"baseline_ok": "true",
		"fault_type":  "concurrency_stress",
	})
}

// TestChaos_PaymentConcurrentCreateIdempotencyKey guards advisory lock under parallel creates.
func TestChaos_PaymentConcurrentCreateIdempotencyKey(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seedCustomer(t, infra.Pool, customerID)

	prov := NewCountingMockProvider()
	svc := NewService(infra.Pool, prov, infra.Cfg)
	key := "chaos-idem-" + uuid.New().String()
	amount := int64(12_000_000)

	var wg sync.WaitGroup
	wg.Add(paymentChaosWorkers)
	for i := 0; i < paymentChaosWorkers; i++ {
		go func() {
			defer wg.Done()
			_, err := svc.CreatePaymentIntent(ctx, customerID, amount, "USD", key, nil)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	var intentCount int
	require.NoError(t, infra.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_intents WHERE idempotency_key = $1`, key).Scan(&intentCount))
	require.Equal(t, 1, intentCount)
	require.Equal(t, 1, prov.Calls())

	logChaosProof(t, "concurrent_idempotency_create", map[string]string{
		"subsystem":      "payment_intent",
		"workers":        itoaPaymentChaos(paymentChaosWorkers),
		"intents":        "1",
		"provider_calls": "1",
		"baseline_ok":    "true",
		"fault_type":     "concurrency_stress",
	})
}

// TestChaos_PaymentConcurrentWebhookSameEventID guards webhook dedup under parallel delivery.
func TestChaos_PaymentConcurrentWebhookSameEventID(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seedCustomer(t, infra.Pool, customerID)

	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	result, err := svc.CreatePaymentIntent(ctx, customerID, 8_000_000, "USD", "chaos-wh-"+uuid.New().String(), nil)
	require.NoError(t, err)
	intent := result.Intent
	providerRef := intent.ProviderRef.String
	eventID := "evt_concurrent_" + uuid.New().String()
	payload := fmt.Sprintf(`{"id":"%s","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":8000000}}}`,
		eventID, providerRef)

	var wg sync.WaitGroup
	wg.Add(paymentChaosWorkers)
	for i := 0; i < paymentChaosWorkers; i++ {
		go func() {
			defer wg.Done()
			_ = svc.ProcessStripeWebhook(ctx, eventID, "payment_intent.succeeded", []byte(payload), providerRef, 8_000_000, payload)
		}()
	}
	wg.Wait()

	var webhookCount int
	require.NoError(t, infra.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.webhook_events WHERE provider = 'stripe' AND provider_event_id = $1`,
		eventID).Scan(&webhookCount))
	require.Equal(t, 1, webhookCount)

	outbox, err := db.New(infra.Pool).GetPendingOutboxEventsForUpdate(ctx, 10)
	require.NoError(t, err)
	require.Len(t, outbox, 1)

	logChaosProof(t, "concurrent_webhook_dedup", map[string]string{
		"subsystem":    "payment_webhook",
		"workers":      itoaPaymentChaos(paymentChaosWorkers),
		"webhook_rows": "1",
		"outbox_rows":  "1",
		"baseline_ok":  "true",
		"fault_type":   "concurrency_stress",
	})
}

// TestChaos_PaymentStaleLeaseReclaim guards expired PROCESSING leases are reclaimed and settled once.
func TestChaos_PaymentStaleLeaseReclaim(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 15_000_000, "chaos-lease-"+uuid.New().String())

	_, err := infra.Pool.Exec(ctx, `
		UPDATE payment.payment_outbox
		SET status = 'PROCESSING', lease_until = now() - interval '1 minute', attempts = 1
		WHERE id = $1`, seed.OutboxID)
	require.NoError(t, err)

	worker := newOutboxWorkerForChaos(infra)
	worker.reclaimStaleProcessing(ctx)
	require.Equal(t, "PENDING", paymentOutboxStatus(t, infra.Pool, seed.OutboxID))

	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assertPaymentChaosInvariants(t, infra.Pool, seed, seed.AmountMicro, 1)

	logChaosProof(t, "stale_lease_reclaim", map[string]string{
		"subsystem":   "payment_outbox",
		"recovered":   "true",
		"ledger_rows": "1",
		"baseline_ok": "true",
		"fault_type":  "worker_crash_simulation",
	})
}

// TestChaos_PaymentPostSettlementMarkGap guards ledger idempotency when mark-processed fails after credit.
func TestChaos_PaymentPostSettlementMarkGap(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()
	defer func() { PostSettlementMarkHook = nil }()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 18_000_000, "chaos-gap-"+uuid.New().String())

	var hookCalls atomic.Int32
	PostSettlementMarkHook = func(ctx context.Context, ev db.PaymentPaymentOutbox) error {
		if hookCalls.Add(1) == 1 {
			return fmt.Errorf("injected post-settlement mark failure")
		}
		return nil
	}

	worker := newOutboxWorkerForChaos(infra)
	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	assertPaymentChaosInvariants(t, infra.Pool, seed, seed.AmountMicro, 1)
	require.Equal(t, "PENDING", paymentOutboxStatus(t, infra.Pool, seed.OutboxID))

	n, err = worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assertPaymentChaosInvariants(t, infra.Pool, seed, seed.AmountMicro, 1)
	require.Equal(t, "PROCESSED", paymentOutboxStatus(t, infra.Pool, seed.OutboxID))

	logChaosProof(t, "post_settlement_mark_failed", map[string]string{
		"subsystem":     "payment_outbox",
		"ledger_rows":   "1",
		"double_credit": "false",
		"hook_calls":    itoaPaymentChaos(int(hookCalls.Load())),
		"baseline_ok":   "true",
		"fault_type":    "injected_timing_gap",
	})
}

// TestChaos_PaymentMissingCustomerSettlementDead guards orphan intents do not credit balance.
func TestChaos_PaymentMissingCustomerSettlementDead(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 9_000_000, "chaos-orphan-"+uuid.New().String())

	_, err := infra.Pool.Exec(ctx, `DELETE FROM customers WHERE id = $1`, ingestion.ToUUID(customerID))
	require.NoError(t, err)

	worker := newOutboxWorkerForChaos(infra)
	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	status := paymentOutboxStatus(t, infra.Pool, seed.OutboxID)
	require.Equal(t, "DEAD", status)
	require.Equal(t, 0, ledgerCountForIntent(t, infra.Pool, seed.IntentID))

	var intentStatus string
	require.NoError(t, infra.Pool.QueryRow(ctx, `
		SELECT status FROM payment.payment_intents WHERE id = $1`, ingestion.ToUUID(seed.IntentID)).Scan(&intentStatus))
	require.Equal(t, "SETTLEMENT_FAILED", intentStatus)

	logChaosProof(t, "settlement_customer_not_found", map[string]string{
		"subsystem":     "payment_outbox",
		"outbox_status": status,
		"intent_status": intentStatus,
		"ledger_rows":   "0",
		"baseline_ok":   "true",
		"fault_type":    "missing_customer",
	})
}

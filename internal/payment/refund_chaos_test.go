package payment

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestChaos_PaymentRefundConcurrentWebhookSameEventID guards refund webhook dedup under parallel delivery.
func TestChaos_PaymentRefundConcurrentWebhookSameEventID(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSettledIntent(t, infra, customerID, 20_000_000, "chaos-ref-wh-"+uuid.New().String())

	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	eventID := "evt_refund_concurrent_" + uuid.New().String()
	refundID := "re_" + uuid.New().String()
	refundAmount := int64(8_000_000)

	var wg sync.WaitGroup
	wg.Add(paymentChaosWorkers)
	for i := 0; i < paymentChaosWorkers; i++ {
		go func() {
			defer wg.Done()
			stripeCents, _ := MicroToStripeAmount(refundAmount)
			payload := fmt.Sprintf(`{"id":"%s","type":"refund.created","data":{"object":{"id":"%s","amount":%d,"payment_intent":"%s","status":"succeeded"}}}`,
				eventID, refundID, stripeCents, seed.ProviderRef)
			_ = svc.ProcessStripeRefundWebhook(ctx, eventID, "refund.created", []byte(payload), refundID, seed.ProviderRef, refundAmount, "succeeded")
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
	require.Equal(t, OutboxEventReverseBalance, outbox[0].EventType)

	logChaosProof(t, "concurrent_refund_webhook_dedup", map[string]string{
		"subsystem":    "payment_refund_webhook",
		"workers":      itoaPaymentChaos(paymentChaosWorkers),
		"webhook_rows": "1",
		"outbox_rows":  "1",
		"baseline_ok":  "true",
		"fault_type":   "concurrency_stress",
	})
}

// TestChaos_PaymentRefundDualOutboxWorkerRace guards concurrent workers debit exactly once per refund outbox row.
func TestChaos_PaymentRefundDualOutboxWorkerRace(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSettledIntent(t, infra, customerID, 22_000_000, "chaos-ref-race-"+uuid.New().String())
	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	outboxID := processRefundWebhook(t, infra.Pool, svc, "evt_ref_race_"+uuid.New().String(), seed.ProviderRef, "re_race_"+uuid.New().String(), 10_000_000)

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

	wantBalance := seed.AmountMicro - 10_000_000
	require.Eventually(t, func() bool {
		return paymentOutboxStatus(t, infra.Pool, outboxID) == "PROCESSED" &&
			ledgerRefundCountForIntent(t, infra.Pool, seed.IntentID) == 1
	}, 10*time.Second, 50*time.Millisecond)
	assertPaymentRefundInvariants(t, infra.Pool, seed, wantBalance, 1)

	logChaosProof(t, "refund_outbox_worker_race", map[string]string{
		"subsystem":   "payment_refund_outbox",
		"workers":     "4",
		"processed":   itoaPaymentChaos(int(totalProcessed.Load())),
		"ledger_rows": "1",
		"baseline_ok": "true",
		"fault_type":  "concurrency_stress",
	})
}

// TestChaos_PaymentRefundPostSettlementMarkGap guards ledger idempotency when mark-processed fails after debit.
func TestChaos_PaymentRefundPostSettlementMarkGap(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()
	defer func() { PostSettlementMarkHook = nil }()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSettledIntent(t, infra, customerID, 24_000_000, "chaos-ref-gap-"+uuid.New().String())
	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	outboxID := processRefundWebhook(t, infra.Pool, svc, "evt_ref_gap_"+uuid.New().String(), seed.ProviderRef, "re_gap_"+uuid.New().String(), 12_000_000)

	var hookCalls atomic.Int32
	PostSettlementMarkHook = func(ctx context.Context, ev db.PaymentPaymentOutbox) error {
		if ev.EventType == OutboxEventReverseBalance && hookCalls.Add(1) == 1 {
			return fmt.Errorf("injected post-refund mark failure")
		}
		return nil
	}

	worker := newOutboxWorkerForChaos(infra)
	wantBalance := seed.AmountMicro - 12_000_000

	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	assertPaymentRefundInvariants(t, infra.Pool, seed, wantBalance, 1)
	require.Equal(t, "PENDING", paymentOutboxStatus(t, infra.Pool, outboxID))

	n, err = worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assertPaymentRefundInvariants(t, infra.Pool, seed, wantBalance, 1)
	require.Equal(t, "PROCESSED", paymentOutboxStatus(t, infra.Pool, outboxID))

	logChaosProof(t, "post_refund_mark_failed", map[string]string{
		"subsystem":    "payment_refund_outbox",
		"ledger_rows":  "1",
		"double_debit": "false",
		"hook_calls":   itoaPaymentChaos(int(hookCalls.Load())),
		"baseline_ok":  "true",
		"fault_type":   "injected_timing_gap",
	})
}

// TestChaos_PaymentPartialRefundThenFull covers two partial refunds without over-debiting the ledger.
func TestChaos_PaymentPartialRefundThenFull(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSettledIntent(t, infra, customerID, 30_000_000, "chaos-partial-"+uuid.New().String())
	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	worker := newOutboxWorkerForChaos(infra)

	outbox1 := processRefundWebhook(t, infra.Pool, svc, "evt_partial_1_"+uuid.New().String(), seed.ProviderRef, "re_partial_1_"+uuid.New().String(), 10_000_000)
	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, "PROCESSED", paymentOutboxStatus(t, infra.Pool, outbox1))
	assertPaymentRefundInvariants(t, infra.Pool, seed, 20_000_000, 1)

	outbox2 := processRefundWebhook(t, infra.Pool, svc, "evt_partial_2_"+uuid.New().String(), seed.ProviderRef, "re_partial_2_"+uuid.New().String(), 20_000_000)
	n, err = worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, "PROCESSED", paymentOutboxStatus(t, infra.Pool, outbox2))
	assertPaymentRefundInvariants(t, infra.Pool, seed, 0, 2)

	var intentStatus string
	require.NoError(t, infra.Pool.QueryRow(ctx, `
		SELECT status FROM payment.payment_intents WHERE id = $1`, ads.ToUUID(seed.IntentID)).Scan(&intentStatus))
	require.Equal(t, "REFUNDED", intentStatus)

	logChaosProof(t, "partial_refund_then_full", map[string]string{
		"subsystem":     "payment_refund",
		"refund_rows":   "2",
		"intent_status": intentStatus,
		"balance":       "0",
		"baseline_ok":   "true",
		"fault_type":    "sequential_partial_refunds",
	})
}

// TestChaos_PaymentRefundExceedsIntentIgnored rejects refund webhooks that would exceed the original intent amount.
func TestChaos_PaymentRefundExceedsIntentIgnored(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	customerID := uuid.New()
	seed := seedSettledIntent(t, infra, customerID, 10_000_000, "chaos-ref-exceed-"+uuid.New().String())
	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)

	stripeCents, err := MicroToStripeAmount(15_000_000)
	require.NoError(t, err)
	eventID := "evt_ref_exceed_" + uuid.New().String()
	payload := fmt.Sprintf(`{"id":"%s","type":"refund.created","data":{"object":{"id":"re_exceed","amount":%d,"payment_intent":"%s","status":"succeeded"}}}`,
		eventID, stripeCents, seed.ProviderRef)
	err = svc.ProcessStripeRefundWebhook(context.Background(), eventID, "refund.created", []byte(payload), "re_exceed", seed.ProviderRef, 15_000_000, "succeeded")
	require.NoError(t, err)

	outbox, err := db.New(infra.Pool).GetPendingOutboxEventsForUpdate(context.Background(), 10)
	require.NoError(t, err)
	require.Empty(t, outbox)
	assertPaymentChaosInvariants(t, infra.Pool, seed, seed.AmountMicro, 1)

	logChaosProof(t, "refund_exceeds_intent_ignored", map[string]string{
		"subsystem":   "payment_refund_webhook",
		"outbox_rows": "0",
		"ledger_rows": "1",
		"baseline_ok": "true",
		"fault_type":  "invalid_refund_amount",
	})
}

// TestChaos_PaymentRefundWithoutTopupDead leaves refund outbox dead when no PAYMENT_TOPUP exists to reverse.
func TestChaos_PaymentRefundWithoutTopupDead(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 9_000_000, "chaos-ref-no-topup-"+uuid.New().String())
	_, err := infra.Pool.Exec(ctx, `DELETE FROM payment.payment_outbox WHERE event_type = 'SETTLE_BALANCE'`)
	require.NoError(t, err)

	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	outboxID := processRefundWebhook(t, infra.Pool, svc, "evt_ref_no_topup_"+uuid.New().String(), seed.ProviderRef, "re_no_topup_"+uuid.New().String(), 9_000_000)

	worker := newOutboxWorkerForChaos(infra)
	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, "DEAD", paymentOutboxStatus(t, infra.Pool, outboxID))
	require.Equal(t, 0, ledgerRefundCountForIntent(t, infra.Pool, seed.IntentID))
	assertPaymentChaosInvariants(t, infra.Pool, seed, 0, 0)

	logChaosProof(t, "refund_without_topup_dead", map[string]string{
		"subsystem":     "payment_refund_outbox",
		"outbox_status": "DEAD",
		"ledger_rows":   "0",
		"baseline_ok":   "true",
		"fault_type":    "missing_topup",
	})
}

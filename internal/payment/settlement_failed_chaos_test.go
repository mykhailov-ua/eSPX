package payment

import (
	"context"
	"fmt"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestChaos_SettlementFailedNotifier enqueues an ops alert when settlement permanently fails.
func TestChaos_SettlementFailedNotifier(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	stub := &stubPaymentNotifierClient{}
	cfg := testPaymentOpsConfig()
	alerter := NewSettlementFailedAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 9_000_000, "chaos-settle-alert-"+uuid.New().String())

	_, err := infra.Pool.Exec(ctx, `DELETE FROM customers WHERE id = $1`, ads.ToUUID(customerID))
	require.NoError(t, err)

	worker := newOutboxWorkerForChaos(infra)
	worker.SetSettlementFailedAlerter(alerter)

	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, "DEAD", paymentOutboxStatus(t, infra.Pool, seed.OutboxID))

	time.Sleep(200 * time.Millisecond)
	requests := stub.snapshot()
	require.Len(t, requests, 1)
	require.Equal(t, "payment-settlement-failed:"+seed.IntentID.String(), requests[0].DedupKey)
	require.Contains(t, requests[0].Body, seed.IntentID.String())

	alerter.AlertPermanentFailure(
		loadPaymentOutboxRow(t, infra.Pool, seed.OutboxID),
		fmt.Errorf("customer not found"),
	)
	time.Sleep(100 * time.Millisecond)
	require.Len(t, stub.snapshot(), 1, "cooldown should suppress duplicate alert for same intent")

	logChaosProof(t, "settlement_failed_notifier", map[string]string{
		"subsystem":    "payment_outbox",
		"intent_id":    seed.IntentID.String(),
		"notified":     "true",
		"dedup_key":    requests[0].DedupKey,
		"baseline_ok":  "true",
		"fault_type":   "missing_customer",
	})
}

func loadPaymentOutboxRow(t *testing.T, pool *pgxpool.Pool, outboxID int64) db.PaymentPaymentOutbox {
	t.Helper()
	var ev db.PaymentPaymentOutbox
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT id, event_type, payload, status, lease_until, attempts, last_error, created_at, processed_at
		FROM payment.payment_outbox WHERE id = $1`, outboxID).Scan(
		&ev.ID, &ev.EventType, &ev.Payload, &ev.Status, &ev.LeaseUntil, &ev.Attempts, &ev.LastError, &ev.CreatedAt, &ev.ProcessedAt,
	))
	return ev
}

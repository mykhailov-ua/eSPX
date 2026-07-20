package payment

import (
	"context"
	"sync"
	"testing"

	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestChaos_CryptoHold_DualWorkerRace ensures concurrent hold workers release exactly once.
func TestChaos_CryptoHold_DualWorkerRace(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seedCustomer(t, infra.Pool, customerID)

	intentID := uuid.New()
	holdID := uuid.New()
	_, err := infra.Pool.Exec(ctx, `
		INSERT INTO payment.payment_intents (
			id, customer_id, amount_micro, currency, status, provider, idempotency_key, metadata
		) VALUES ($1, $2, 25_000_000, 'USDT', 'SUCCEEDED', 'crypto', $3, '{}'::jsonb)`,
		ingestion.ToUUID(intentID), ingestion.ToUUID(customerID), "chaos-hold-"+uuid.NewString())
	require.NoError(t, err)

	_, err = infra.Pool.Exec(ctx, `
		INSERT INTO payment.crypto_holds (
			id, payment_intent_id, customer_id, amount_micro, currency, tx_hash, status, release_at
		) VALUES ($1, $2, $3, 25_000_000, 'USDT', '0xrace', 'HELD', now() - interval '1 second')`,
		ingestion.ToUUID(holdID), ingestion.ToUUID(intentID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	worker := NewCryptoHoldWorker(infra.Pool, infra.Cfg)
	const workers = 4
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_ = worker.ProcessHolds(ctx)
		}()
	}
	wg.Wait()

	var holdStatus string
	err = infra.Pool.QueryRow(ctx, `SELECT status FROM payment.crypto_holds WHERE id = $1`, ingestion.ToUUID(holdID)).Scan(&holdStatus)
	require.NoError(t, err)
	require.Equal(t, "RELEASED", holdStatus)

	var outboxCount int
	err = infra.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_outbox WHERE event_type = 'SETTLE_BALANCE'`).Scan(&outboxCount)
	require.NoError(t, err)
	require.Equal(t, 1, outboxCount)

	logChaosProof(t, "crypto_hold_dual_worker", map[string]string{
		"subsystem":   "payment_crypto_hold",
		"workers":     "4",
		"outbox_rows": "1",
		"hold_status": holdStatus,
		"baseline_ok": "true",
		"fault_type":  "concurrency_stress",
	})
}

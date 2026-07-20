package payment

import (
	"context"
	"os"
	"strings"
	"testing"

	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestExplainAudit_PaymentIntentQueries runs EXPLAIN on payment intent hot-path SQL (M-DB-PG-5).
func TestExplainAudit_PaymentIntentQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping payment EXPLAIN audit in short mode")
	}
	if os.Getenv("EXPLAIN_AUDIT") == "" {
		t.Skip("set EXPLAIN_AUDIT=1 to run payment query plan audit")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	idempotencyKey := "explain-audit-payment-key"
	intentID := uuid.New()

	_, err := infra.Pool.Exec(ctx, `
		INSERT INTO payment.payment_intents (
			id, customer_id, amount_micro, currency, status, provider, idempotency_key, metadata
		) VALUES ($1, $2, 5000000, 'USD', 'CREATED', 'stripe', $3, '{}'::jsonb)
		ON CONFLICT DO NOTHING`,
		ingestion.ToUUID(intentID), ingestion.ToUUID(uuid.New()), idempotencyKey)
	require.NoError(t, err)

	_, err = infra.Pool.Exec(ctx, `
		INSERT INTO payment.crypto_holds (
			id, payment_intent_id, customer_id, amount_micro, currency, tx_hash, status, release_at
		) VALUES ($1, $2, $3, 5000000, 'USDT', '0xexplain', 'HELD', now() - interval '1 minute')
		ON CONFLICT DO NOTHING`,
		ingestion.ToUUID(uuid.New()), ingestion.ToUUID(intentID), ingestion.ToUUID(uuid.New()))
	require.NoError(t, err)

	queries := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "payment.GetPaymentIntentByIdempotencyKey",
			sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM payment.payment_intents WHERE idempotency_key = $1`,
			args: []any{idempotencyKey},
		},
		{
			name: "payment.CreatePaymentIntent",
			sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO payment.payment_intents (
  id, customer_id, amount_micro, currency, status, provider, provider_ref, idempotency_key, metadata
) VALUES ($1, $2, $3, 'USD', 'CREATED', 'stripe', NULL, $4, '{}'::jsonb)`,
			args: []any{ingestion.ToUUID(uuid.New()), ingestion.ToUUID(uuid.New()), int64(1_000_000), "explain-insert-" + uuid.NewString()},
		},
		{
			name: "payment.FinalizePaymentIntentCheckout",
			sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE payment.payment_intents
SET status = 'PENDING_PROVIDER', provider_ref = $2, metadata = '{}'::jsonb, updated_at = now()
WHERE id = $1`,
			args: []any{ingestion.ToUUID(intentID), "pi_explain_ref"},
		},
		{
			name: "payment.ClaimCryptoHoldForUpdate",
			sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, payment_intent_id, customer_id, amount_micro, currency, tx_hash, status, release_at
FROM payment.crypto_holds
WHERE status = 'HELD' AND release_at <= now()
ORDER BY release_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED`,
		},
	}

	for _, qc := range queries {
		rows, err := infra.Pool.Query(ctx, qc.sql, qc.args...)
		require.NoError(t, err, qc.name)
		var lines []string
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			lines = append(lines, line)
		}
		rows.Close()
		require.NoError(t, rows.Err(), qc.name)
		plan := strings.Join(lines, "\n")
		t.Logf("=== %s ===\n%s", qc.name, plan)
		require.NotContains(t, plan, "Seq Scan on payment_intents", qc.name)
	}
}

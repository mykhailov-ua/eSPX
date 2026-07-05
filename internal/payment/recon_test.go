package payment

import (
	"context"
	"testing"
	"time"

	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestFinancialReconRun_persistsRunAndFindings stores reconciliation output in payment.financial_recon_* tables.
func TestFinancialReconRun_persistsRunAndFindings(t *testing.T) {
	if testing.Short() {
		t.Skip("requires testcontainers")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	recon := NewReconService(pool, pool, nil)
	end := time.Now().UTC()
	summary, err := recon.Run(context.Background(), end.Add(-time.Hour), end)
	require.NoError(t, err)
	require.NotZero(t, summary.RunID)

	var status string
	var findingsCount int
	err = pool.QueryRow(context.Background(), `
		SELECT status, findings_count FROM payment.financial_recon_runs WHERE id = $1`, summary.RunID).
		Scan(&status, &findingsCount)
	require.NoError(t, err)
	require.Equal(t, "COMPLETED", status)
	require.Equal(t, summary.FindingsCount, findingsCount)
}

// TestFinancialReconRun_missingTopupAfterWebhook detects ledger gap for succeeded-but-unsettled intents.
func TestFinancialReconRun_missingTopupAfterWebhook(t *testing.T) {
	if testing.Short() {
		t.Skip("requires testcontainers")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seedCustomer(t, pool, customerID)

	svc := NewService(pool, NewMockProvider(), nil)
	result, err := svc.CreatePaymentIntent(ctx, customerID, 11_000_000, "USD", "recon-unit-"+uuid.New().String(), nil)
	require.NoError(t, err)
	providerRef := result.Intent.ProviderRef.String
	payload := `{"id":"evt_recon_unit","type":"payment_intent.succeeded","data":{"object":{"id":"` + providerRef + `","amount":11000000}}}`
	err = svc.ProcessStripeWebhook(ctx, "evt_recon_unit", "payment_intent.succeeded", []byte(payload), providerRef, 11_000_000, payload)
	require.NoError(t, err)

	recon := NewReconService(pool, pool, nil)
	end := time.Now().UTC()
	summary, err := recon.Run(ctx, end.Add(-time.Hour), end)
	require.NoError(t, err)
	require.Equal(t, 1, countReconFindingsByKind(t, pool, summary.RunID, db.PaymentFinancialFindingKindMISSINGLEDGERTOPUP))
}

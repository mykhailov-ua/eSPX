package payment

import (
	"context"
	"sync"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func newReconForChaos(infra *paymentChaosInfra) *ReconService {
	return NewReconService(infra.Pool, infra.Pool, nil)
}

func countReconFindingsByKind(t *testing.T, pool *pgxpool.Pool, runID int64, kind db.PaymentFinancialFindingKind) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM payment.financial_recon_findings
		WHERE run_id = $1 AND kind = $2`, runID, kind).Scan(&n)
	require.NoError(t, err)
	return n
}

// TestChaos_FinancialReconCleanSettlement reports zero findings when top-up ledger matches settled intents.
func TestChaos_FinancialReconCleanSettlement(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	seedSettledIntent(t, infra, uuid.New(), 18_000_000, "chaos-recon-clean-"+uuid.New().String())

	recon := newReconForChaos(infra)
	end := time.Now().UTC()
	summary, err := recon.Run(context.Background(), end.Add(-time.Hour), end)
	require.NoError(t, err)
	require.Equal(t, 0, summary.FindingsCount)
	require.GreaterOrEqual(t, summary.IntentsChecked, 1)

	logChaosProof(t, "financial_recon_clean_settlement", map[string]string{
		"subsystem":       "payment_financial_recon",
		"findings":        "0",
		"intents_checked": itoaPaymentChaos(summary.IntentsChecked),
		"baseline_ok":     "true",
		"fault_type":      "none",
	})
}

// TestChaos_FinancialReconMissingTopup flags SUCCEEDED intents without a PAYMENT_TOPUP ledger row.
func TestChaos_FinancialReconMissingTopup(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	seedSucceededIntentWithOutbox(t, infra, uuid.New(), 12_000_000, "chaos-recon-miss-"+uuid.New().String())

	recon := newReconForChaos(infra)
	end := time.Now().UTC()
	summary, err := recon.Run(context.Background(), end.Add(-time.Hour), end)
	require.NoError(t, err)
	require.GreaterOrEqual(t, summary.TopupMissing, 1)
	require.Equal(t, 1, countReconFindingsByKind(t, infra.Pool, summary.RunID, db.PaymentFinancialFindingKindMISSINGLEDGERTOPUP))

	logChaosProof(t, "financial_recon_missing_topup", map[string]string{
		"subsystem":     "payment_financial_recon",
		"findings":      itoaPaymentChaos(summary.FindingsCount),
		"topup_missing": itoaPaymentChaos(summary.TopupMissing),
		"baseline_ok":   "true",
		"fault_type":    "missing_topup",
	})
}

// TestChaos_FinancialReconDeadOutbox surfaces DEAD outbox rows from failed refund settlement.
func TestChaos_FinancialReconDeadOutbox(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, 9_000_000, "chaos-recon-dead-"+uuid.New().String())
	_, err := infra.Pool.Exec(ctx, `DELETE FROM payment.payment_outbox WHERE event_type = 'SETTLE_BALANCE'`)
	require.NoError(t, err)

	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	processRefundWebhook(t, infra.Pool, svc, "evt_recon_dead_"+uuid.New().String(), seed.ProviderRef, "re_recon_dead_"+uuid.New().String(), 9_000_000)

	worker := newOutboxWorkerForChaos(infra)
	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	recon := newReconForChaos(infra)
	end := time.Now().UTC()
	summary, err := recon.Run(ctx, end.Add(-time.Hour), end)
	require.NoError(t, err)
	require.GreaterOrEqual(t, summary.DeadOutboxRows, 1)
	require.Equal(t, 1, countReconFindingsByKind(t, infra.Pool, summary.RunID, db.PaymentFinancialFindingKindDEADOUTBOX))

	logChaosProof(t, "financial_recon_dead_outbox", map[string]string{
		"subsystem":      "payment_financial_recon",
		"dead_outbox":    itoaPaymentChaos(summary.DeadOutboxRows),
		"findings":       itoaPaymentChaos(summary.FindingsCount),
		"baseline_ok":    "true",
		"fault_type":     "dead_outbox",
	})
}

// TestChaos_FinancialReconRefundDrift flags payment_refunds totals that diverge from PAYMENT_REFUND ledger.
func TestChaos_FinancialReconRefundDrift(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	customerID := uuid.New()
	seed := seedSettledIntent(t, infra, customerID, 20_000_000, "chaos-recon-drift-"+uuid.New().String())
	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	processRefundWebhook(t, infra.Pool, svc, "evt_recon_drift_"+uuid.New().String(), seed.ProviderRef, "re_recon_drift_"+uuid.New().String(), 6_000_000)

	recon := newReconForChaos(infra)
	end := time.Now().UTC()
	summary, err := recon.Run(context.Background(), end.Add(-time.Hour), end)
	require.NoError(t, err)
	require.Equal(t, 1, countReconFindingsByKind(t, infra.Pool, summary.RunID, db.PaymentFinancialFindingKindREFUNDLEDGERDRIFT))

	logChaosProof(t, "financial_recon_refund_drift", map[string]string{
		"subsystem":   "payment_financial_recon",
		"findings":    itoaPaymentChaos(summary.FindingsCount),
		"drift_kind":  "REFUND_LEDGER_DRIFT",
		"baseline_ok": "true",
		"fault_type":  "ledger_drift",
	})
}

// TestChaos_FinancialReconSettlementFailedIntent reports intents stuck in SETTLEMENT_FAILED.
func TestChaos_FinancialReconSettlementFailedIntent(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	_ = seedSucceededIntentWithOutbox(t, infra, customerID, 9_000_000, "chaos-recon-fail-"+uuid.New().String())
	_, err := infra.Pool.Exec(ctx, `DELETE FROM customers WHERE id = $1`, ads.ToUUID(customerID))
	require.NoError(t, err)

	worker := newOutboxWorkerForChaos(infra)
	n, err := worker.ProcessOutbox(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	recon := newReconForChaos(infra)
	end := time.Now().UTC()
	summary, err := recon.Run(ctx, end.Add(-time.Hour), end)
	require.NoError(t, err)
	require.GreaterOrEqual(t, summary.SettlementFailed, 1)
	require.Equal(t, 1, countReconFindingsByKind(t, infra.Pool, summary.RunID, db.PaymentFinancialFindingKindSETTLEMENTFAILEDINTENT))

	logChaosProof(t, "financial_recon_settlement_failed", map[string]string{
		"subsystem":          "payment_financial_recon",
		"settlement_failed":  itoaPaymentChaos(summary.SettlementFailed),
		"findings":           itoaPaymentChaos(summary.FindingsCount),
		"baseline_ok":        "true",
		"fault_type":         "settlement_failed",
	})
}

// TestChaos_FinancialReconConcurrentRuns allows parallel reconciliation passes without corrupting run rows.
func TestChaos_FinancialReconConcurrentRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	seedSettledIntent(t, infra, uuid.New(), 15_000_000, "chaos-recon-conc-"+uuid.New().String())
	recon := newReconForChaos(infra)
	end := time.Now().UTC()
	start := end.Add(-time.Hour)

	const workers = 4
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, _ = recon.Run(context.Background(), start, end)
		}()
	}
	wg.Wait()

	var runCount int
	require.NoError(t, infra.Pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM payment.financial_recon_runs WHERE status = 'COMPLETED'`).Scan(&runCount))
	require.Equal(t, workers, runCount)

	logChaosProof(t, "financial_recon_concurrent_runs", map[string]string{
		"subsystem":   "payment_financial_recon",
		"workers":     "4",
		"runs":        itoaPaymentChaos(runCount),
		"baseline_ok": "true",
		"fault_type":  "concurrency_stress",
	})
}

// TestChaos_FinancialReconOpsAlert enqueues a notifier message when WARN+ findings are detected.
func TestChaos_FinancialReconOpsAlert(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupPaymentChaosInfra(t)
	defer cleanup()

	stub := &stubPaymentNotifierClient{}
	cfg := testPaymentOpsConfig()
	alerter := NewFinancialReconAlerter(&NotifierClient{client: stub}, cfg)
	require.NotNil(t, alerter)

	seedSucceededIntentWithOutbox(t, infra, uuid.New(), 11_000_000, "chaos-recon-ops-"+uuid.New().String())
	recon := NewReconService(infra.Pool, infra.Pool, alerter)

	end := time.Now().UTC()
	summary, err := recon.Run(context.Background(), end.Add(-time.Hour), end)
	require.NoError(t, err)
	require.GreaterOrEqual(t, summary.FindingsCount, 1)

	time.Sleep(200 * time.Millisecond)
	requests := stub.snapshot()
	require.Len(t, requests, 1)
	require.NotEmpty(t, requests[0].DedupKey)
	require.Contains(t, requests[0].Body, "MISSING_LEDGER_TOPUP")

	logChaosProof(t, "financial_recon_ops_alert", map[string]string{
		"subsystem":    "payment_financial_recon",
		"findings":     itoaPaymentChaos(summary.FindingsCount),
		"notified":     "true",
		"baseline_ok":  "true",
		"fault_type":   "missing_topup",
	})
}

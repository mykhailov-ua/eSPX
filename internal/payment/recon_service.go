package payment

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"espx/internal/payment/db"
	"espx/pkg/coldpath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReconService compares payment schema state against ads balance_ledger for finance reporting.
type ReconService struct {
	paymentPool *pgxpool.Pool
	ledger      *SettlementLedgerClient
	alerter     *FinancialReconAlerter

	wg sync.WaitGroup
}

// NewReconService wires payment pool and settlement ledger reader for financial recon.
func NewReconService(paymentPool *pgxpool.Pool, ledger *SettlementLedgerClient, alerter *FinancialReconAlerter) *ReconService {
	return &ReconService{paymentPool: paymentPool, ledger: ledger, alerter: alerter}
}

// StartWorker runs financial reconciliation on a fixed interval until ctx is cancelled.
func (recon *ReconService) StartWorker(ctx context.Context, interval time.Duration) {
	recon.wg.Add(1)
	defer recon.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			end := time.Now().UTC()
			start := end.Add(-interval)
			if _, err := recon.Run(ctx, start, end); err != nil {
				slog.Error("payment financial recon failed", "error", err)
			}
		}
	}
}

// Wait blocks until the recon worker goroutine exits.
func (recon *ReconService) Wait() {
	recon.wg.Wait()
}

// Run executes one financial reconciliation pass and persists findings.
func (recon *ReconService) Run(ctx context.Context, periodStart, periodEnd time.Time) (FinancialReconSummary, error) {
	var summary FinancialReconSummary
	summary.PeriodStart = periodStart
	summary.PeriodEnd = periodEnd
	summary.FindingsByKind = make(map[string]int)

	run, err := db.New(recon.paymentPool).CreateFinancialReconRun(ctx, db.CreateFinancialReconRunParams{
		PeriodStart: pgtype.Timestamptz{Time: periodStart, Valid: true},
		PeriodEnd:   pgtype.Timestamptz{Time: periodEnd, Valid: true},
	})
	if err != nil {
		return summary, fmt.Errorf("create financial recon run: %w", err)
	}
	summary.RunID = run.ID

	findings, intentsChecked, err := recon.collectFindings(ctx)
	if err != nil {
		_ = db.New(recon.paymentPool).FailFinancialReconRun(ctx, db.FailFinancialReconRunParams{
			ID:           run.ID,
			ErrorMessage: pgtype.Text{String: err.Error(), Valid: true},
		})
		return summary, err
	}
	summary.IntentsChecked = intentsChecked

	err = pgx.BeginFunc(ctx, recon.paymentPool, func(tx pgx.Tx) error {
		q := db.New(tx)
		for _, f := range findings {
			detailBytes, err := coldpath.MarshalJSON(f.Detail)
			if err != nil {
				return fmt.Errorf("marshal recon finding detail: %w", err)
			}
			var intentUUID pgtype.UUID
			if f.PaymentIntentID != uuid.Nil {
				intentUUID = pgtype.UUID{Bytes: f.PaymentIntentID, Valid: true}
			}
			var custUUID pgtype.UUID
			if f.CustomerID != uuid.Nil {
				custUUID = pgtype.UUID{Bytes: f.CustomerID, Valid: true}
			}
			_, err = q.CreateFinancialReconFinding(ctx, db.CreateFinancialReconFindingParams{
				RunID:              run.ID,
				Kind:               f.Kind,
				PaymentIntentID:    intentUUID,
				CustomerID:         custUUID,
				PaymentAmountMicro: f.PaymentAmountMicro,
				LedgerAmountMicro:  f.LedgerAmountMicro,
				DeltaMicro:         f.DeltaMicro,
				Detail:             detailBytes,
			})
			if err != nil {
				return err
			}
			summary.FindingsByKind[string(f.Kind)]++
			switch f.Kind {
			case db.PaymentFinancialFindingKindMISSINGLEDGERTOPUP:
				summary.TopupMissing++
			case db.PaymentFinancialFindingKindDEADOUTBOX:
				summary.DeadOutboxRows++
			case db.PaymentFinancialFindingKindSETTLEMENTFAILEDINTENT:
				summary.SettlementFailed++
			default:
			}
		}
		return q.CompleteFinancialReconRun(ctx, db.CompleteFinancialReconRunParams{
			ID:             run.ID,
			FindingsCount:  int32(len(findings)),
			IntentsChecked: int32(intentsChecked),
		})
	})
	if err != nil {
		_ = db.New(recon.paymentPool).FailFinancialReconRun(ctx, db.FailFinancialReconRunParams{
			ID:           run.ID,
			ErrorMessage: pgtype.Text{String: err.Error(), Valid: true},
		})
		FinancialReconRunsTotal.WithLabelValues("failed").Inc()
		return summary, err
	}

	for kind, count := range summary.FindingsByKind {
		FinancialReconFindingsTotal.WithLabelValues(kind).Add(float64(count))
	}
	FinancialReconRunsTotal.WithLabelValues("completed").Inc()

	summary.FindingsCount = len(findings)
	summary.TopupAligned = intentsChecked - summary.TopupMissing - summary.SettlementFailed
	if summary.TopupAligned < 0 {
		summary.TopupAligned = 0
	}

	slog.Info("payment financial recon completed",
		"run_id", run.ID,
		"findings", len(findings),
		"intents_checked", intentsChecked,
	)
	if recon.alerter != nil {
		recon.alerter.AlertFindings(summary, findings)
	}
	return summary, nil
}

func (recon *ReconService) collectFindings(ctx context.Context) ([]FinancialReconFinding, int, error) {
	q := db.New(recon.paymentPool)
	intents, err := q.ListIntentsForFinancialRecon(ctx)
	if err != nil {
		return nil, 0, err
	}

	topups := make(map[uuid.UUID]int64)
	refundLedger := make(map[uuid.UUID]int64)
	chargebackLedger := make(map[uuid.UUID]int64)
	reversalLedger := make(map[uuid.UUID]int64)

	paymentRefunds, err := q.SumRefundsByIntentForRecon(ctx)
	if err != nil {
		return nil, 0, err
	}
	refundByIntent := make(map[uuid.UUID]int64, len(paymentRefunds))
	for _, row := range paymentRefunds {
		refundByIntent[uuid.UUID(row.PaymentIntentID.Bytes)] = row.RefundMicro
	}

	disputes, err := q.SumDisputeWithdrawnByIntentForRecon(ctx)
	if err != nil {
		return nil, 0, err
	}
	disputeByIntent := make(map[uuid.UUID]struct{ withdrawn, reinstated int64 })
	for _, row := range disputes {
		disputeByIntent[uuid.UUID(row.PaymentIntentID.Bytes)] = struct{ withdrawn, reinstated int64 }{
			withdrawn:  row.WithdrawnMicro,
			reinstated: row.ReinstatedMicro,
		}
	}

	var findings []FinancialReconFinding
	seenTopupIntents := make(map[uuid.UUID]struct{}, len(intents))

	for _, intent := range intents {
		intentID := uuid.UUID(intent.ID.Bytes)
		customerID := uuid.UUID(intent.CustomerID.Bytes)
		seenTopupIntents[intentID] = struct{}{}

		if recon.ledger != nil {
			ledgerState, ledgerErr := recon.ledger.GetPaymentIntentLedger(ctx, intentID)
			if ledgerErr != nil {
				return nil, 0, ledgerErr
			}
			if ledgerState.HasTopup {
				topups[intentID] = ledgerState.TopupMicro
			}
			refundLedger[intentID] = ledgerState.RefundMicro
			chargebackLedger[intentID] = ledgerState.ChargebackMicro
			reversalLedger[intentID] = ledgerState.ChargebackReversalMicro
		}

		if intent.Status == db.PaymentPaymentIntentStatusSETTLEMENTFAILED {
			findings = append(findings, FinancialReconFinding{
				Kind:               db.PaymentFinancialFindingKindSETTLEMENTFAILEDINTENT,
				PaymentIntentID:    intentID,
				CustomerID:         customerID,
				PaymentAmountMicro: intent.AmountMicro,
				Detail:             map[string]any{"status": string(intent.Status)},
			})
			continue
		}

		topupMicro, hasTopup := topups[intentID]
		switch {
		case !hasTopup || topupMicro == 0:
			findings = append(findings, FinancialReconFinding{
				Kind:               db.PaymentFinancialFindingKindMISSINGLEDGERTOPUP,
				PaymentIntentID:    intentID,
				CustomerID:         customerID,
				PaymentAmountMicro: intent.AmountMicro,
				LedgerAmountMicro:  topupMicro,
				DeltaMicro:         intent.AmountMicro - topupMicro,
			})
		case topupMicro != intent.AmountMicro:
			findings = append(findings, FinancialReconFinding{
				Kind:               db.PaymentFinancialFindingKindTOPUPAMOUNTMISMATCH,
				PaymentIntentID:    intentID,
				CustomerID:         customerID,
				PaymentAmountMicro: intent.AmountMicro,
				LedgerAmountMicro:  topupMicro,
				DeltaMicro:         intent.AmountMicro - topupMicro,
			})
		}

		if payRefund := refundByIntent[intentID]; payRefund > 0 {
			ledgerRefund := refundLedger[intentID]
			if payRefund != ledgerRefund {
				findings = append(findings, FinancialReconFinding{
					Kind:               db.PaymentFinancialFindingKindREFUNDLEDGERDRIFT,
					PaymentIntentID:    intentID,
					CustomerID:         customerID,
					PaymentAmountMicro: payRefund,
					LedgerAmountMicro:  ledgerRefund,
					DeltaMicro:         payRefund - ledgerRefund,
				})
			}
		}

		if dp, ok := disputeByIntent[intentID]; ok {
			if dp.withdrawn > 0 && chargebackLedger[intentID] != dp.withdrawn {
				findings = append(findings, FinancialReconFinding{
					Kind:               db.PaymentFinancialFindingKindCHARGEBACKLEDGERDRIFT,
					PaymentIntentID:    intentID,
					CustomerID:         customerID,
					PaymentAmountMicro: dp.withdrawn,
					LedgerAmountMicro:  chargebackLedger[intentID],
					DeltaMicro:         dp.withdrawn - chargebackLedger[intentID],
				})
			}
			if dp.reinstated > 0 && reversalLedger[intentID] != dp.reinstated {
				findings = append(findings, FinancialReconFinding{
					Kind:               db.PaymentFinancialFindingKindCHARGEBACKREVERSALDRIFT,
					PaymentIntentID:    intentID,
					CustomerID:         customerID,
					PaymentAmountMicro: dp.reinstated,
					LedgerAmountMicro:  reversalLedger[intentID],
					DeltaMicro:         dp.reinstated - reversalLedger[intentID],
				})
			}
		}
	}

	for intentID, topupMicro := range topups {
		if _, ok := seenTopupIntents[intentID]; !ok && topupMicro != 0 {
			findings = append(findings, FinancialReconFinding{
				Kind:              db.PaymentFinancialFindingKindORPHANLEDGERTOPUP,
				PaymentIntentID:   intentID,
				LedgerAmountMicro: topupMicro,
				DeltaMicro:        topupMicro,
				Detail:            map[string]any{"orphan_topup_micro": topupMicro},
			})
		}
	}

	deadOutbox, err := q.ListDeadOutboxEventsForRecon(ctx)
	if err != nil {
		return nil, 0, err
	}
	for _, row := range deadOutbox {
		findings = append(findings, FinancialReconFinding{
			Kind:               db.PaymentFinancialFindingKindDEADOUTBOX,
			PaymentAmountMicro: 0,
			Detail: map[string]any{
				"outbox_id":  row.ID,
				"event_type": row.EventType,
				"last_error": row.LastError.String,
				"attempts":   row.Attempts,
			},
		})
	}

	return findings, len(intents), nil
}

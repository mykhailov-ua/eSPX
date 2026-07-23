package management

import (
	"context"
	"encoding/json"
	"fmt"

	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// ApplyReconciliationAdjust executes a RECONCILIATION_ADJUST outbox payload (M3-10).
func (w *OutboxWorker) ApplyReconciliationAdjust(ctx context.Context, eventID int64, payload []byte) error {
	p, err := parseReconciliationAdjustPayload(payload)
	if err != nil {
		return err
	}
	campID, err := uuid.Parse(p.CampaignID)
	if err != nil {
		return fmt.Errorf("invalid campaign id: %w", err)
	}
	customerID, err := uuid.Parse(p.CustomerID)
	if err != nil {
		return fmt.Errorf("invalid customer id: %w", err)
	}

	tx, err := w.svc.GetPool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := db.New(tx)
	_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
		CustomerID: pgUUID(customerID),
		CampaignID: ingestion.ToUUID(campID),
		Amount:     p.LedgerAmt,
		Type:       db.LedgerTypeRECONCILIATIONADJUST,
	})
	if err != nil {
		return err
	}

	spendDelta := -p.LedgerAmt
	if spendDelta != 0 {
		if err := q.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
			ID:           ingestion.ToUUID(campID),
			CurrentSpend: spendDelta,
		}); err != nil {
			return err
		}
	}

	if p.RedisDelta != 0 {
		if int(p.ShardID) >= len(w.svc.rdbs) {
			return fmt.Errorf("invalid shard_id %d", p.ShardID)
		}
		rdb := w.svc.rdbs[p.ShardID]
		recon := NewReconService(w.svc)
		if err := recon.adjustRedisBudgetAtomically(ctx, rdb, campID, p.RedisDelta); err != nil {
			return err
		}
	}

	if p.RunID > 0 {
		_, err = tx.Exec(ctx, `
			UPDATE recon_discrepancies
			SET redis_adjusted = true
			WHERE run_id = $1 AND campaign_id = $2`,
			p.RunID, ingestion.ToUUID(campID),
		)
		if err != nil {
			return err
		}
	}

	adminID := uuid.MustParse(quotaRepairSystemAdmin)
	w.svc.AuditLog(ctx, q, adminID, "RECONCILIATION_ADJUST", "campaign",
		&campID, p, map[string]any{"outbox_event_id": eventID})

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	metrics.ReconCorrectionsAppliedTotal.Inc()
	return nil
}

func parseReconciliationAdjustPayload(payload []byte) (ReconciliationAdjustPayload, error) {
	var p ReconciliationAdjustPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return p, err
	}
	if p.CampaignID == "" || p.CustomerID == "" {
		return p, fmt.Errorf("invalid reconciliation adjust payload")
	}
	if p.LedgerAmt == 0 && p.RedisDelta == 0 {
		return p, fmt.Errorf("empty reconciliation adjust")
	}
	return p, nil
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}

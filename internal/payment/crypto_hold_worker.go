package payment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"espx/internal/config"
	"espx/internal/payment/db"
	"espx/pkg/coldpath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CryptoHoldWorker periodically processes HELD crypto top-ups after their 14-day hold period.
type CryptoHoldWorker struct {
	pool *pgxpool.Pool
	cfg  *config.Config
	wg   sync.WaitGroup
}

// NewCryptoHoldWorker creates a new worker instance.
func NewCryptoHoldWorker(pool *pgxpool.Pool, cfg *config.Config) *CryptoHoldWorker {
	return &CryptoHoldWorker{
		pool: pool,
		cfg:  cfg,
	}
}

// Start runs the background polling loop for releasing holds.
func (w *CryptoHoldWorker) Start(ctx context.Context, interval time.Duration) {
	w.wg.Add(1)
	defer w.wg.Done()

	slog.Info("crypto hold worker starting polling loop", "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.ProcessHolds(ctx); err != nil {
				slog.Error("failed to process crypto holds", "error", err)
			}
		}
	}
}

type cryptoHold struct {
	ID              uuid.UUID
	PaymentIntentID uuid.UUID
	CustomerID      uuid.UUID
	AmountMicro     int64
	Currency        string
	TxHash          string
	Status          string
	ReleaseAt       time.Time
}

// ProcessHolds scans for eligible holds, evaluates the fraud gate, and releases funds.
func (w *CryptoHoldWorker) ProcessHolds(ctx context.Context) error {
	for {
		var processed bool
		err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error {
			var hold cryptoHold
			err := tx.QueryRow(ctx, `
				SELECT id, payment_intent_id, customer_id, amount_micro, currency, tx_hash, status, release_at
				FROM payment.crypto_holds
				WHERE status = 'HELD' AND release_at <= now()
				ORDER BY release_at ASC
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			`).Scan(&hold.ID, &hold.PaymentIntentID, &hold.CustomerID, &hold.AmountMicro, &hold.Currency, &hold.TxHash, &hold.Status, &hold.ReleaseAt)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil
				}
				return fmt.Errorf("claim crypto hold: %w", err)
			}
			processed = true

			txQueries := db.New(tx)

			var isFraudulent bool
			err = tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM payment.payment_disputes d
					JOIN payment.payment_intents i ON d.payment_intent_id = i.id
					WHERE i.customer_id = $1 AND d.status = 'OPEN'
				) OR EXISTS (
					SELECT 1 FROM payment.payment_intents
					WHERE customer_id = $1 AND status IN ('FAILED', 'DISPUTED')
				)
			`, hold.CustomerID).Scan(&isFraudulent)
			if err != nil {
				return fmt.Errorf("fraud gate evaluation failed: %w", err)
			}

			if isFraudulent {
				slog.Warn("crypto hold blocked by fraud gate", "hold_id", hold.ID, "customer_id", hold.CustomerID)
				_, err = tx.Exec(ctx, `
					UPDATE payment.crypto_holds
					SET status = 'FRAUD_BLOCKED', updated_at = now()
					WHERE id = $1
				`, hold.ID)
				return err
			}

			_, err = tx.Exec(ctx, `
				UPDATE payment.crypto_holds
				SET status = 'RELEASED', updated_at = now()
				WHERE id = $1
			`, hold.ID)
			if err != nil {
				return fmt.Errorf("update hold status: %w", err)
			}

			outboxPayload := map[string]any{
				"customer_id":            hold.CustomerID.String(),
				"amount_micro":           hold.AmountMicro,
				"ledger_idempotency_key": ledgerIdempotencyKey(hold.PaymentIntentID),
				"payment_intent_id":      hold.PaymentIntentID.String(),
				"provider":               "crypto",
				"provider_ref":           "tx_crypto_" + hold.PaymentIntentID.String(),
			}
			payloadJSON, err := coldpath.MarshalJSON(outboxPayload)
			if err != nil {
				return fmt.Errorf("marshal settle balance outbox payload: %w", err)
			}

			_, err = txQueries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
				EventType: "SETTLE_BALANCE",
				Payload:   payloadJSON,
			})
			if err != nil {
				return fmt.Errorf("enqueue settle balance event: %w", err)
			}

			slog.Info("crypto hold released successfully", "hold_id", hold.ID, "customer_id", hold.CustomerID, "amount_micro", hold.AmountMicro)
			return nil
		})
		if err != nil {
			return err
		}
		if !processed {
			return nil
		}
	}
}

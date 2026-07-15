package payment

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"

	"espx/internal/config"
	"espx/internal/payment/db"
	"espx/pkg/coldpath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Service owns intent persistence and webhook state transitions in the payment schema.
type Service struct {
	pool     *pgxpool.Pool
	provider Provider
	cfg      *config.Config
}

// NewService binds Postgres and a checkout provider so intent persistence and provider calls share one lifecycle.
func NewService(pool *pgxpool.Pool, prov Provider, cfg *config.Config) *Service {
	return &Service{
		pool:     pool,
		provider: prov,
		cfg:      cfg,
	}
}

// CreateIntentResult carries checkout URL alongside the persisted intent for idempotent gRPC and HTMX responses.
type CreateIntentResult struct {
	Intent      db.PaymentPaymentIntent
	CheckoutURL string
}

// CreatePaymentIntent uses an advisory lock and idempotency key so concurrent retries cannot double-create checkouts.
func (service *Service) CreatePaymentIntent(ctx context.Context, customerID uuid.UUID, amountMicro int64, currency string, idempotencyKey string, metadata map[string]string) (CreateIntentResult, error) {
	conn, err := service.pool.Acquire(ctx)
	if err != nil {
		return CreateIntentResult{}, fmt.Errorf("acquire conn for idempotency lock: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1::text, 0))`, idempotencyKey); err != nil {
		return CreateIntentResult{}, fmt.Errorf("idempotency lock: %w", err)
	}
	defer func() {
		unlockCtx := context.WithoutCancel(ctx)
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1::text, 0))`, idempotencyKey)
	}()

	q := db.New(conn)

	existing, err := q.GetPaymentIntentByIdempotencyKey(ctx, idempotencyKey)
	if err == nil {
		IntentsTotal.WithLabelValues(string(existing.Status)).Inc()
		return reconcileIdempotentIntent(existing, customerID, amountMicro, currency)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CreateIntentResult{}, fmt.Errorf("failed to lookup payment intent: %w", err)
	}

	provRef, checkoutURL, err := service.provider.CreateCheckout(ctx, amountMicro, currency, metadata, idempotencyKey)
	if err != nil {
		if errors.Is(err, ErrProviderNotConfigured) {
			return CreateIntentResult{}, err
		}
		return CreateIntentResult{}, fmt.Errorf("%w: %w", ErrCheckoutUnavailable, err)
	}

	intentID, err := uuid.NewV7()
	if err != nil {
		return CreateIntentResult{}, fmt.Errorf("generate payment intent id: %w", err)
	}
	metaBytes, err := mergeIntentMetadata(metadata, checkoutURL)
	if err != nil {
		return CreateIntentResult{}, fmt.Errorf("failed to encode intent metadata: %w", err)
	}

	status := db.PaymentPaymentIntentStatusCREATED
	if provRef != "" {
		status = db.PaymentPaymentIntentStatusPENDINGPROVIDER
	}

	var intent db.PaymentPaymentIntent
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		txQueries := db.New(tx)
		var innerErr error
		intent, innerErr = txQueries.CreatePaymentIntent(ctx, db.CreatePaymentIntentParams{
			ID:             pgtype.UUID{Bytes: intentID, Valid: true},
			CustomerID:     pgtype.UUID{Bytes: customerID, Valid: true},
			AmountMicro:    amountMicro,
			Currency:       currency,
			Status:         status,
			Provider:       service.provider.Name(),
			ProviderRef:    pgtype.Text{String: provRef, Valid: provRef != ""},
			IdempotencyKey: idempotencyKey,
			Metadata:       metaBytes,
		})
		return innerErr
	})
	if err != nil {
		if coldpath.IsUniqueViolation(err) {
			existing, lookupErr := q.GetPaymentIntentByIdempotencyKey(ctx, idempotencyKey)
			if lookupErr != nil {
				return CreateIntentResult{}, fmt.Errorf("idempotency race recovery failed: %w", lookupErr)
			}
			return reconcileIdempotentIntent(existing, customerID, amountMicro, currency)
		}
		return CreateIntentResult{}, fmt.Errorf("failed to insert payment intent: %w", err)
	}

	IntentsTotal.WithLabelValues(string(intent.Status)).Inc()
	return CreateIntentResult{Intent: intent, CheckoutURL: checkoutURL}, nil
}

// reconcileIdempotentIntent enforces that an idempotency key cannot be reused with different money fields.
func reconcileIdempotentIntent(existing db.PaymentPaymentIntent, customerID uuid.UUID, amountMicro int64, currency string) (CreateIntentResult, error) {
	existCust := uuid.UUID(existing.CustomerID.Bytes)
	if existCust != customerID || existing.AmountMicro != amountMicro || existing.Currency != currency {
		return CreateIntentResult{}, fmt.Errorf("%w: existing intent has customer=%s amount=%d currency=%s", ErrIdempotencyConflict, existCust, existing.AmountMicro, existing.Currency)
	}
	return CreateIntentResult{Intent: existing, CheckoutURL: checkoutURLFromIntent(existing)}, nil
}

// GetPaymentIntent is a thin read path for status polling and admin lookups.
func (s *Service) GetPaymentIntent(ctx context.Context, intentID uuid.UUID) (db.PaymentPaymentIntent, error) {
	intent, err := db.New(s.pool).GetPaymentIntent(ctx, pgtype.UUID{Bytes: intentID, Valid: true})
	if err != nil {
		return db.PaymentPaymentIntent{}, mapNotFound(err, ErrPaymentIntentNotFound)
	}
	return intent, nil
}

// ListPaymentIntents returns paginated history because customer ledgers can accumulate many intents over time.
func (s *Service) ListPaymentIntents(ctx context.Context, customerID uuid.UUID, limit, offset int32) ([]db.PaymentPaymentIntent, int64, error) {
	q := db.New(s.pool)
	custUUID := pgtype.UUID{Bytes: customerID, Valid: true}
	listParams := db.ListPaymentIntentsParams{
		CustomerID: custUUID,
		Limit:      limit,
		Offset:     offset,
	}
	return coldpath.PaginatedQuery(
		func() (int64, error) { return q.CountPaymentIntents(ctx, custUUID) },
		func() ([]db.PaymentPaymentIntent, error) { return q.ListPaymentIntents(ctx, listParams) },
	)
}

// isValidTransition blocks backward moves that could enqueue a second settlement or mask chargebacks.
func isValidTransition(oldStatus, newStatus db.PaymentPaymentIntentStatus) bool {
	if oldStatus == newStatus {
		return true
	}
	switch oldStatus {
	case db.PaymentPaymentIntentStatusCREATED:
		return true
	case db.PaymentPaymentIntentStatusPENDINGPROVIDER:
		return newStatus != db.PaymentPaymentIntentStatusCREATED
	case db.PaymentPaymentIntentStatusPROCESSING:
		return newStatus != db.PaymentPaymentIntentStatusCREATED &&
			newStatus != db.PaymentPaymentIntentStatusPENDINGPROVIDER
	case db.PaymentPaymentIntentStatusSUCCEEDED:
		return newStatus == db.PaymentPaymentIntentStatusREFUNDED ||
			newStatus == db.PaymentPaymentIntentStatusDISPUTED
	case db.PaymentPaymentIntentStatusREFUNDED:
		return newStatus == db.PaymentPaymentIntentStatusDISPUTED
	case db.PaymentPaymentIntentStatusDISPUTED:
		return newStatus == db.PaymentPaymentIntentStatusSUCCEEDED
	case db.PaymentPaymentIntentStatusFAILED,
		db.PaymentPaymentIntentStatusCANCELLED,
		db.PaymentPaymentIntentStatusSETTLEMENTFAILED:
		return false
	default:
		return true
	}
}

// ledgerIdempotencyKey is derived from intent id so settlement retries cannot mint duplicate ledger rows.
func ledgerIdempotencyKey(intentID uuid.UUID) string {
	return "payment:" + intentID.String()
}

// ProcessStripeWebhook commits intent status and outbox enqueue in one transaction so settlement
// is never queued for a webhook that did not durably record success.
func (service *Service) ProcessStripeWebhook(ctx context.Context, eventID string, eventType string, payload []byte, providerRef string, amountMicro int64, rawEvent string) error {
	h := sha256.New()
	h.Write(payload)
	payloadHash := h.Sum(nil)

	redactedBytes, err := coldpath.RedactStripeWebhookPayload(payload)
	if err != nil {
		return fmt.Errorf("redact stripe webhook payload: %w", err)
	}

	err = pgx.BeginFunc(ctx, service.pool, func(tx pgx.Tx) error {
		txQueries := db.New(tx)

		_, err := txQueries.GetWebhookEvent(ctx, db.GetWebhookEventParams{
			Provider:        "stripe",
			ProviderEventID: eventID,
		})
		if err == nil {
			slog.Info("webhook event already processed", "event_id", eventID)
			WebhookEventsTotal.WithLabelValues("duplicate").Inc()
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		_, err = txQueries.CreateWebhookEvent(ctx, db.CreateWebhookEventParams{
			Provider:        "stripe",
			ProviderEventID: eventID,
			EventType:       eventType,
			PayloadHash:     payloadHash,
			PayloadRedacted: redactedBytes,
			Status:          db.PaymentWebhookEventStatusRECEIVED,
			ErrorMessage:    pgtype.Text{},
		})
		if err != nil {
			if coldpath.IsUniqueViolation(err) {
				slog.Info("webhook event deduplicated by unique constraint", "event_id", eventID)
				WebhookEventsTotal.WithLabelValues("duplicate").Inc()
				return nil
			}
			return err
		}

		var intent db.PaymentPaymentIntent
		err = tx.QueryRow(ctx, `
			SELECT id, customer_id, amount_micro, currency, status, provider, provider_ref, idempotency_key, metadata, created_at, updated_at
			FROM payment.payment_intents
			WHERE provider = 'stripe' AND provider_ref = $1
			FOR UPDATE`, providerRef).Scan(
			&intent.ID, &intent.CustomerID, &intent.AmountMicro, &intent.Currency, &intent.Status, &intent.Provider, &intent.ProviderRef, &intent.IdempotencyKey, &intent.Metadata, &intent.CreatedAt, &intent.UpdatedAt,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				slog.Warn("received stripe event for unknown provider_ref", "provider_ref", providerRef)
				return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "unknown provider_ref")
			}
			return err
		}

		var targetStatus db.PaymentPaymentIntentStatus
		switch eventType {
		case "payment_intent.succeeded":
			targetStatus = db.PaymentPaymentIntentStatusSUCCEEDED
		case "payment_intent.payment_failed":
			targetStatus = db.PaymentPaymentIntentStatusFAILED
		case "payment_intent.canceled":
			targetStatus = db.PaymentPaymentIntentStatusCANCELLED
		default:
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "")
		}

		if targetStatus == db.PaymentPaymentIntentStatusSUCCEEDED && amountMicro <= 0 {
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "zero or negative amount")
		}

		if amountMicro != intent.AmountMicro {
			slog.Warn("webhook amount mismatch", "intent_id", uuid.UUID(intent.ID.Bytes), "intent_amount", intent.AmountMicro, "webhook_amount", amountMicro)
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "amount mismatch")
		}

		if !isValidTransition(intent.Status, targetStatus) {
			slog.Warn("invalid state transition skipped", "intent_id", uuid.UUID(intent.ID.Bytes), "from", intent.Status, "to", targetStatus)
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED,
				fmt.Sprintf("invalid transition from %s to %s", intent.Status, targetStatus))
		}

		alreadySettled := intent.Status == db.PaymentPaymentIntentStatusSUCCEEDED

		_, err = txQueries.UpdatePaymentIntentStatus(ctx, db.UpdatePaymentIntentStatusParams{
			ID:          intent.ID,
			Status:      targetStatus,
			ProviderRef: pgtype.Text{String: providerRef, Valid: true},
		})
		if err != nil {
			return err
		}

		if targetStatus == db.PaymentPaymentIntentStatusSUCCEEDED && !alreadySettled {
			intentUUID := uuid.UUID(intent.ID.Bytes)
			outboxPayload := map[string]any{
				"customer_id":            uuid.UUID(intent.CustomerID.Bytes).String(),
				"amount_micro":           intent.AmountMicro,
				"ledger_idempotency_key": ledgerIdempotencyKey(intentUUID),
				"payment_intent_id":      intentUUID.String(),
				"provider":               "stripe",
				"provider_ref":           providerRef,
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
				return err
			}
		}

		return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "")
	})
	if err == nil {
		WebhookEventsTotal.WithLabelValues("processed").Inc()
	}
	return err
}

// ProcessStripeRefundWebhook records a Stripe refund and enqueues REVERSE_BALANCE when funds are returned.
func (service *Service) ProcessStripeRefundWebhook(ctx context.Context, eventID string, eventType string, payload []byte, providerRefundID string, paymentIntentRef string, refundAmountMicro int64, refundStatus string) error {
	h := sha256.New()
	h.Write(payload)
	payloadHash := h.Sum(nil)

	redactedBytes, err := coldpath.RedactStripeWebhookPayload(payload)
	if err != nil {
		return fmt.Errorf("redact stripe webhook payload: %w", err)
	}

	err = pgx.BeginFunc(ctx, service.pool, func(tx pgx.Tx) error {
		txQueries := db.New(tx)

		_, err := txQueries.GetWebhookEvent(ctx, db.GetWebhookEventParams{
			Provider:        "stripe",
			ProviderEventID: eventID,
		})
		if err == nil {
			slog.Info("refund webhook event already processed", "event_id", eventID)
			WebhookEventsTotal.WithLabelValues("duplicate").Inc()
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		_, err = txQueries.CreateWebhookEvent(ctx, db.CreateWebhookEventParams{
			Provider:        "stripe",
			ProviderEventID: eventID,
			EventType:       eventType,
			PayloadHash:     payloadHash,
			PayloadRedacted: redactedBytes,
			Status:          db.PaymentWebhookEventStatusRECEIVED,
			ErrorMessage:    pgtype.Text{},
		})
		if err != nil {
			if coldpath.IsUniqueViolation(err) {
				slog.Info("refund webhook event deduplicated by unique constraint", "event_id", eventID)
				WebhookEventsTotal.WithLabelValues("duplicate").Inc()
				return nil
			}
			return err
		}

		if refundStatus == "failed" {
			_, lookupErr := txQueries.GetPaymentRefundByProviderRefundID(ctx, db.GetPaymentRefundByProviderRefundIDParams{
				Provider:         "stripe",
				ProviderRefundID: providerRefundID,
			})
			if lookupErr == nil {
				return txQueries.UpdatePaymentRefundStatus(ctx, db.UpdatePaymentRefundStatusParams{
					Provider:         "stripe",
					ProviderRefundID: providerRefundID,
					Status:           db.PaymentRefundStatusFAILED,
				})
			}
			if !errors.Is(lookupErr, pgx.ErrNoRows) {
				return lookupErr
			}
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "refund failed before settlement")
		}

		if refundStatus != "succeeded" {
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "refund not yet succeeded")
		}

		if refundAmountMicro <= 0 {
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "zero or negative refund amount")
		}

		var intent db.PaymentPaymentIntent
		err = tx.QueryRow(ctx, `
			SELECT id, customer_id, amount_micro, currency, status, provider, provider_ref, idempotency_key, metadata, refunded_amount_micro, created_at, updated_at
			FROM payment.payment_intents
			WHERE provider = 'stripe' AND provider_ref = $1
			FOR UPDATE`, paymentIntentRef).Scan(
			&intent.ID, &intent.CustomerID, &intent.AmountMicro, &intent.Currency, &intent.Status, &intent.Provider, &intent.ProviderRef, &intent.IdempotencyKey, &intent.Metadata, &intent.RefundedAmountMicro, &intent.CreatedAt, &intent.UpdatedAt,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				slog.Warn("received stripe refund for unknown payment_intent", "payment_intent", paymentIntentRef)
				return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "unknown payment_intent")
			}
			return err
		}

		if intent.Status != db.PaymentPaymentIntentStatusSUCCEEDED && intent.Status != db.PaymentPaymentIntentStatusREFUNDED {
			slog.Warn("refund webhook for non-settled intent", "intent_id", uuid.UUID(intent.ID.Bytes), "status", intent.Status)
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED,
				fmt.Sprintf("intent status %s not refundable", intent.Status))
		}

		if intent.RefundedAmountMicro+refundAmountMicro > intent.AmountMicro {
			slog.Warn("refund would exceed intent amount", "intent_id", uuid.UUID(intent.ID.Bytes),
				"refunded", intent.RefundedAmountMicro, "delta", refundAmountMicro, "intent_amount", intent.AmountMicro)
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "refund exceeds intent amount")
		}

		existingRefund, err := txQueries.GetPaymentRefundByProviderRefundID(ctx, db.GetPaymentRefundByProviderRefundIDParams{
			Provider:         "stripe",
			ProviderRefundID: providerRefundID,
		})
		if err == nil {
			if existingRefund.Status == db.PaymentRefundStatusSUCCEEDED {
				return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "")
			}
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "duplicate refund in non-success state")
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		refundID, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generate refund id: %w", err)
		}
		_, err = txQueries.CreatePaymentRefund(ctx, db.CreatePaymentRefundParams{
			ID:               pgtype.UUID{Bytes: refundID, Valid: true},
			PaymentIntentID:  intent.ID,
			Provider:         "stripe",
			ProviderRefundID: providerRefundID,
			AmountMicro:      refundAmountMicro,
			Status:           db.PaymentRefundStatusSUCCEEDED,
		})
		if err != nil {
			if coldpath.IsUniqueViolation(err) {
				return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "")
			}
			return err
		}

		_, err = txQueries.ApplyIntentRefundAmount(ctx, db.ApplyIntentRefundAmountParams{
			ID:                  intent.ID,
			RefundedAmountMicro: refundAmountMicro,
		})
		if err != nil {
			return err
		}

		intentUUID := uuid.UUID(intent.ID.Bytes)
		customerUUID := uuid.UUID(intent.CustomerID.Bytes)
		outboxPayload, err := coldpath.MarshalJSON(reverseBalancePayload(intentUUID, customerUUID, refundAmountMicro, providerRefundID))
		if err != nil {
			return fmt.Errorf("marshal reverse balance outbox payload: %w", err)
		}
		_, err = txQueries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: OutboxEventReverseBalance,
			Payload:   outboxPayload,
		})
		if err != nil {
			return err
		}

		return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "")
	})
	if err == nil {
		WebhookEventsTotal.WithLabelValues("processed").Inc()
	}
	return err
}

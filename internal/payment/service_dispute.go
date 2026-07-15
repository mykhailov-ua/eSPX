package payment

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"

	"espx/internal/payment/db"
	"espx/pkg/coldpath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func disputeClosedStatus(stripeStatus string) (db.PaymentDisputeStatus, bool) {
	switch stripeStatus {
	case "won":
		return db.PaymentDisputeStatusWON, true
	case "lost", "charge_refunded":
		return db.PaymentDisputeStatusLOST, true
	default:
		return "", false
	}
}

func intentStatusAllowsDispute(status db.PaymentPaymentIntentStatus) bool {
	return status == db.PaymentPaymentIntentStatusSUCCEEDED ||
		status == db.PaymentPaymentIntentStatusREFUNDED ||
		status == db.PaymentPaymentIntentStatusDISPUTED
}

// ProcessStripeDisputeWebhook records dispute lifecycle events and enqueues chargeback settlement outbox rows.
func (service *Service) ProcessStripeDisputeWebhook(
	ctx context.Context,
	eventID string,
	eventType string,
	payload []byte,
	providerDisputeID string,
	paymentIntentRef string,
	disputeAmountMicro int64,
	stripeDisputeStatus string,
) error {
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
			slog.Info("dispute webhook event already processed", "event_id", eventID)
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
				WebhookEventsTotal.WithLabelValues("duplicate").Inc()
				return nil
			}
			return err
		}

		if paymentIntentRef == "" {
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "missing payment_intent")
		}
		if disputeAmountMicro <= 0 && eventType != "charge.dispute.closed" && eventType != "charge.dispute.updated" {
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "zero or negative dispute amount")
		}

		intent, err := lockIntentByProviderRef(ctx, tx, paymentIntentRef)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "unknown payment_intent")
			}
			return err
		}

		if !intentStatusAllowsDispute(intent.Status) {
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED,
				fmt.Sprintf("intent status %s not disputable", intent.Status))
		}

		dispute, disputeErr := txQueries.GetPaymentDisputeByProviderDisputeID(ctx, db.GetPaymentDisputeByProviderDisputeIDParams{
			Provider:          "stripe",
			ProviderDisputeID: providerDisputeID,
		})
		hasDispute := disputeErr == nil
		if disputeErr != nil && !errors.Is(disputeErr, pgx.ErrNoRows) {
			return disputeErr
		}

		switch eventType {
		case "charge.dispute.created":
			if hasDispute {
				break
			}
			disputeID, err := uuid.NewV7()
			if err != nil {
				return fmt.Errorf("generate dispute id: %w", err)
			}
			amount := disputeAmountMicro
			if amount <= 0 {
				amount = intent.AmountMicro
			}
			_, err = txQueries.CreatePaymentDispute(ctx, db.CreatePaymentDisputeParams{
				ID:                pgtype.UUID{Bytes: disputeID, Valid: true},
				PaymentIntentID:   intent.ID,
				Provider:          "stripe",
				ProviderDisputeID: providerDisputeID,
				AmountMicro:       amount,
				Status:            db.PaymentDisputeStatusOPEN,
			})
			if err != nil {
				if coldpath.IsUniqueViolation(err) {
					break
				}
				return err
			}
			if intent.Status != db.PaymentPaymentIntentStatusDISPUTED {
				if !isValidTransition(intent.Status, db.PaymentPaymentIntentStatusDISPUTED) {
					return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "invalid transition to DISPUTED")
				}
				_, err = txQueries.UpdatePaymentIntentStatus(ctx, db.UpdatePaymentIntentStatusParams{
					ID:     intent.ID,
					Status: db.PaymentPaymentIntentStatusDISPUTED,
				})
				if err != nil {
					return err
				}
			}

		case "charge.dispute.funds_withdrawn":
			if !hasDispute {
				if err := service.ensureDisputeRow(ctx, txQueries, intent, providerDisputeID, disputeAmountMicro); err != nil {
					return err
				}
				dispute, err = txQueries.GetPaymentDisputeByProviderDisputeID(ctx, db.GetPaymentDisputeByProviderDisputeIDParams{
					Provider: "stripe", ProviderDisputeID: providerDisputeID,
				})
				if err != nil {
					return err
				}
			}
			if dispute.WithdrawnAmountMicro >= disputeAmountMicro {
				break
			}
			delta := disputeAmountMicro - dispute.WithdrawnAmountMicro
			if dispute.WithdrawnAmountMicro+delta > intent.AmountMicro {
				return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "chargeback exceeds intent amount")
			}
			_, err = txQueries.ApplyDisputeFundsWithdrawn(ctx, db.ApplyDisputeFundsWithdrawnParams{
				Provider:             "stripe",
				ProviderDisputeID:    providerDisputeID,
				WithdrawnAmountMicro: delta,
			})
			if err != nil {
				return err
			}
			if intent.Status != db.PaymentPaymentIntentStatusDISPUTED {
				_, err = txQueries.UpdatePaymentIntentStatus(ctx, db.UpdatePaymentIntentStatusParams{
					ID: intent.ID, Status: db.PaymentPaymentIntentStatusDISPUTED,
				})
				if err != nil {
					return err
				}
			}
			outboxPayload, err := coldpath.MarshalJSON(applyChargebackPayload(
				uuid.UUID(intent.ID.Bytes), uuid.UUID(intent.CustomerID.Bytes), delta, providerDisputeID,
			))
			if err != nil {
				return fmt.Errorf("marshal apply chargeback outbox payload: %w", err)
			}
			_, err = txQueries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
				EventType: OutboxEventApplyChargeback,
				Payload:   outboxPayload,
			})
			if err != nil {
				return err
			}

		case "charge.dispute.funds_reinstated":
			if !hasDispute {
				return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "dispute not found for reinstatement")
			}
			if dispute.ReinstatedAmountMicro >= disputeAmountMicro {
				break
			}
			delta := disputeAmountMicro - dispute.ReinstatedAmountMicro
			if dispute.ReinstatedAmountMicro+delta > dispute.WithdrawnAmountMicro {
				return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusIGNORED, "reinstatement exceeds withdrawn amount")
			}
			_, err = txQueries.ApplyDisputeFundsReinstated(ctx, db.ApplyDisputeFundsReinstatedParams{
				Provider:              "stripe",
				ProviderDisputeID:     providerDisputeID,
				ReinstatedAmountMicro: delta,
			})
			if err != nil {
				return err
			}
			outboxPayload, err := coldpath.MarshalJSON(reverseChargebackPayload(
				uuid.UUID(intent.ID.Bytes), uuid.UUID(intent.CustomerID.Bytes), delta, providerDisputeID,
			))
			if err != nil {
				return fmt.Errorf("marshal reverse chargeback outbox payload: %w", err)
			}
			_, err = txQueries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
				EventType: OutboxEventReverseChargeback,
				Payload:   outboxPayload,
			})
			if err != nil {
				return err
			}

		case "charge.dispute.closed", "charge.dispute.updated":
			if closed, ok := disputeClosedStatus(stripeDisputeStatus); ok {
				if hasDispute {
					_ = txQueries.ClosePaymentDispute(ctx, db.ClosePaymentDisputeParams{
						Provider: "stripe", ProviderDisputeID: providerDisputeID, Status: closed,
					})
				}
				if closed == db.PaymentDisputeStatusWON && intent.Status == db.PaymentPaymentIntentStatusDISPUTED {
					_, err = txQueries.UpdatePaymentIntentStatus(ctx, db.UpdatePaymentIntentStatusParams{
						ID: intent.ID, Status: db.PaymentPaymentIntentStatusSUCCEEDED,
					})
					if err != nil {
						return err
					}
				}
			} else if hasDispute && eventType == "charge.dispute.updated" {
				_ = txQueries.UpdatePaymentDisputeStatus(ctx, db.UpdatePaymentDisputeStatusParams{
					Provider: "stripe", ProviderDisputeID: providerDisputeID, Status: db.PaymentDisputeStatusOPEN,
				})
			}

		default:
			return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "unhandled dispute event")
		}

		return updateStripeWebhookStatus(ctx, txQueries, eventID, db.PaymentWebhookEventStatusPROCESSED, "")
	})
	if err == nil {
		WebhookEventsTotal.WithLabelValues("processed").Inc()
	}
	return err
}

func lockIntentByProviderRef(ctx context.Context, tx pgx.Tx, paymentIntentRef string) (db.PaymentPaymentIntent, error) {
	var intent db.PaymentPaymentIntent
	err := tx.QueryRow(ctx, `
		SELECT id, customer_id, amount_micro, currency, status, provider, provider_ref, idempotency_key, metadata, refunded_amount_micro, created_at, updated_at
		FROM payment.payment_intents
		WHERE provider = 'stripe' AND provider_ref = $1
		FOR UPDATE`, paymentIntentRef).Scan(
		&intent.ID, &intent.CustomerID, &intent.AmountMicro, &intent.Currency, &intent.Status,
		&intent.Provider, &intent.ProviderRef, &intent.IdempotencyKey, &intent.Metadata,
		&intent.RefundedAmountMicro, &intent.CreatedAt, &intent.UpdatedAt,
	)
	return intent, err
}

func (service *Service) ensureDisputeRow(ctx context.Context, q db.Querier, intent db.PaymentPaymentIntent, providerDisputeID string, amountMicro int64) error {
	disputeID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate dispute id: %w", err)
	}
	amount := amountMicro
	if amount <= 0 {
		amount = intent.AmountMicro
	}
	_, err = q.CreatePaymentDispute(ctx, db.CreatePaymentDisputeParams{
		ID:                pgtype.UUID{Bytes: disputeID, Valid: true},
		PaymentIntentID:   intent.ID,
		Provider:          "stripe",
		ProviderDisputeID: providerDisputeID,
		AmountMicro:       amount,
		Status:            db.PaymentDisputeStatusOPEN,
	})
	if err != nil && !coldpath.IsUniqueViolation(err) {
		return err
	}
	if intent.Status != db.PaymentPaymentIntentStatusDISPUTED {
		_, err = q.UpdatePaymentIntentStatus(ctx, db.UpdatePaymentIntentStatusParams{
			ID: intent.ID, Status: db.PaymentPaymentIntentStatusDISPUTED,
		})
	}
	return err
}

package payment

import (
	"context"
	"fmt"

	"espx/internal/payment/db"
	"espx/pkg/coldpath"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// DisputeListItem is the service-layer view of a disputed payment intent.
type DisputeListItem struct {
	Intent            db.PaymentPaymentIntent
	ProviderDisputeID string
}

// ListDisputes returns paginated disputed intents, optionally scoped to one customer.
func (service *Service) ListDisputes(ctx context.Context, customerID *uuid.UUID, limit, offset int32) ([]DisputeListItem, int64, error) {
	q := db.New(service.pool)
	var cust pgtype.UUID
	if customerID != nil && *customerID != uuid.Nil {
		cust = pgtype.UUID{Bytes: *customerID, Valid: true}
	}
	listParams := db.ListDisputedPaymentIntentsParams{
		Limit:      limit,
		Offset:     offset,
		CustomerID: cust,
	}
	intents, total, err := coldpath.PaginatedQuery(
		func() (int64, error) { return q.CountDisputedPaymentIntents(ctx, cust) },
		func() ([]db.PaymentPaymentIntent, error) { return q.ListDisputedPaymentIntents(ctx, listParams) },
	)
	if err != nil {
		return nil, 0, err
	}

	items := make([]DisputeListItem, 0, len(intents))
	for _, intent := range intents {
		item := DisputeListItem{Intent: intent}
		dispute, derr := q.GetLatestDisputeForIntent(ctx, intent.ID)
		if derr == nil {
			item.ProviderDisputeID = dispute.ProviderDisputeID
		}
		items = append(items, item)
	}
	return items, total, nil
}

// ReplayWebhook re-drives Stripe processing from a stored redacted payload with settlement idempotency intact.
func (service *Service) ReplayWebhook(ctx context.Context, provider, providerEventID string) (string, error) {
	if provider != "stripe" {
		return "", fmt.Errorf("%w: unsupported provider %q", ErrInvalidRequestBody, provider)
	}
	if providerEventID == "" {
		return "", ErrInvalidRequestBody
	}

	q := db.New(service.pool)
	ev, err := q.GetWebhookEvent(ctx, db.GetWebhookEventParams{
		Provider:        provider,
		ProviderEventID: providerEventID,
	})
	if err != nil {
		return "", mapNotFound(err, ErrWebhookEventNotFound)
	}
	if len(ev.PayloadRedacted) == 0 {
		return "", fmt.Errorf("%w: webhook payload_redacted is empty", ErrInvalidRequestBody)
	}

	switch ev.Status {
	case db.PaymentWebhookEventStatusPROCESSED, db.PaymentWebhookEventStatusIGNORED:
		return "already_processed", nil
	}

	body := ev.PayloadRedacted
	event, err := coldpath.DecodeBody[stripeEvent](body)
	if err != nil {
		return "", fmt.Errorf("decode stored webhook payload: %w", err)
	}
	if event.ID == "" {
		event.ID = providerEventID
	}
	if event.Type == "" {
		event.Type = ev.EventType
	}

	if err := service.dispatchStripeWebhook(ctx, event, body); err != nil {
		return "", err
	}
	return "processed", nil
}

func (service *Service) dispatchStripeWebhook(ctx context.Context, event stripeEvent, body []byte) error {
	switch event.Type {
	case "refund.created", "refund.updated", "refund.failed":
		providerRefundID := event.Data.Object.ID
		paymentIntentRef := event.Data.Object.PaymentIntent
		refundStatus := event.Data.Object.Status
		if event.Type == "refund.failed" {
			refundStatus = "failed"
		}
		return service.ProcessStripeRefundWebhook(
			ctx, event.ID, event.Type, body, providerRefundID, paymentIntentRef,
			StripeAmountToMicro(event.Data.Object.Amount), refundStatus,
		)
	case "charge.dispute.created", "charge.dispute.updated", "charge.dispute.closed",
		"charge.dispute.funds_withdrawn", "charge.dispute.funds_reinstated":
		return service.ProcessStripeDisputeWebhook(
			ctx, event.ID, event.Type, body, event.Data.Object.ID, event.Data.Object.PaymentIntent,
			StripeAmountToMicro(event.Data.Object.Amount), event.Data.Object.Status,
		)
	default:
		providerRef := event.Data.Object.ID
		if providerRef == "" {
			return fmt.Errorf("%w: stripe event missing provider ref", ErrInvalidRequestBody)
		}
		return service.ProcessStripeWebhook(
			ctx, event.ID, event.Type, body, providerRef,
			StripeAmountToMicro(event.Data.Object.Amount), string(body),
		)
	}
}

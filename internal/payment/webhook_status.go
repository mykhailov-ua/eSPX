package payment

import (
	"context"

	"espx/internal/payment/db"

	"github.com/jackc/pgx/v5/pgtype"
)

// updateStripeWebhookStatus records webhook processing outcome for a Stripe event id.
func updateStripeWebhookStatus(ctx context.Context, q db.Querier, eventID string, status db.PaymentWebhookEventStatus, errMsg string) error {
	var msg pgtype.Text
	if errMsg != "" {
		msg = pgtype.Text{String: errMsg, Valid: true}
	}
	return q.UpdateWebhookEventStatus(ctx, db.UpdateWebhookEventStatusParams{
		Provider:        "stripe",
		ProviderEventID: eventID,
		Status:          status,
		ErrorMessage:    msg,
	})
}

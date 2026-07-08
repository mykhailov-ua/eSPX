package payment

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrProviderNotConfigured signals Stripe checkout is unavailable (missing secret key or URLs).
	ErrProviderNotConfigured = errors.New("stripe provider not configured")
	// ErrIdempotencyConflict is returned when an idempotency key is reused with different money fields.
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
	// ErrCheckoutUnavailable wraps provider or network failures creating a checkout session.
	ErrCheckoutUnavailable = errors.New("checkout unavailable")
	// ErrCustomerNotFound is returned when the ads customer row does not exist.
	ErrCustomerNotFound = errors.New("customer not found")
	// ErrInvalidAmount signals zero or negative payment amounts.
	ErrInvalidAmount = errors.New("invalid payment amount")
	// ErrInvalidCustomerID signals malformed customer UUID input.
	ErrInvalidCustomerID = errors.New("invalid customer id")
	// ErrInvalidRequestBody signals decode failures on HTMX or JSON bodies.
	ErrInvalidRequestBody = errors.New("invalid request body")
	// ErrInvalidIntentID signals malformed payment intent UUID input.
	ErrInvalidIntentID = errors.New("invalid intent id")
	// ErrPaymentIntentNotFound is returned when no payment intent row exists.
	ErrPaymentIntentNotFound = errors.New("payment intent not found")
	// ErrWebhookEventNotFound is returned when no stored webhook row exists for replay.
	ErrWebhookEventNotFound = errors.New("webhook event not found")
)

// mapNotFound maps pgx.ErrNoRows to a domain not-found sentinel; other errors pass through.
func mapNotFound(err error, notFound error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound
	}
	return err
}

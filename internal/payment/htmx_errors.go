package payment

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Public error codes returned in data-code; never expose internal driver or RPC text.
const (
	CodeInvalidInput    = "PAYMENT_INVALID_INPUT"
	CodeInvalidAmount   = "PAYMENT_INVALID_AMOUNT"
	CodeInvalidCustomer = "PAYMENT_INVALID_CUSTOMER"
	CodeConflict        = "PAYMENT_CONFLICT"
	CodeUnavailable     = "PAYMENT_UNAVAILABLE"
	CodeNotFound        = "PAYMENT_NOT_FOUND"
	CodeForbidden       = "PAYMENT_FORBIDDEN"
	CodeFailed          = "PAYMENT_FAILED"
)

// MapHTMXError maps domain failures to stable codes because HTMX clients must not see driver or RPC text.
func MapHTMXError(err error) (status int, code, message string) {
	if err == nil {
		return 0, "", ""
	}

	msg := err.Error()
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return StatusNotFound, CodeNotFound, "Payment resource was not found."
	case strings.Contains(msg, "idempotency key conflict"):
		return StatusConflict, CodeConflict, "This payment request conflicts with an earlier attempt. Start a new checkout."
	case strings.Contains(msg, "amount_micro must be greater than zero"),
		strings.Contains(msg, "amount is required"),
		strings.Contains(msg, "amount must be positive"):
		return StatusValidation, CodeInvalidAmount, "Enter a valid payment amount."
	case strings.Contains(msg, "invalid customer id"),
		strings.Contains(msg, "customer not found"):
		return StatusNotFound, CodeInvalidCustomer, "Account not found."
	case strings.Contains(msg, "idempotency_key is required"),
		strings.Contains(msg, "invalid request body"),
		strings.Contains(msg, "invalid intent id"):
		return StatusValidation, CodeInvalidInput, "Check your payment details and try again."
	case strings.Contains(msg, "failed to create checkout session"),
		strings.Contains(msg, "stripe provider not configured"),
		strings.Contains(msg, "provider"):
		return StatusUnavailable, CodeUnavailable, "Payments are temporarily unavailable. Try again shortly."
	case strings.Contains(msg, "forbidden"),
		strings.Contains(msg, "unauthorized"):
		return StatusForbidden, CodeForbidden, "You are not allowed to perform this payment."
	default:
		return StatusFailed, CodeFailed, "Something went wrong processing your payment. Try again or contact support."
	}
}

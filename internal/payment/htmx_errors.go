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

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return StatusNotFound, CodeNotFound, "Payment resource was not found."
	case errors.Is(err, ErrIdempotencyConflict):
		return StatusConflict, CodeConflict, "This payment request conflicts with an earlier attempt. Start a new checkout."
	case errors.Is(err, ErrInvalidAmount):
		return StatusValidation, CodeInvalidAmount, "Enter a valid payment amount."
	case errors.Is(err, ErrInvalidCustomerID):
		return StatusNotFound, CodeInvalidCustomer, "Account not found."
	case errors.Is(err, ErrCustomerNotFound):
		return StatusNotFound, CodeInvalidCustomer, "Account not found."
	case errors.Is(err, ErrInvalidRequestBody), errors.Is(err, ErrInvalidIntentID):
		return StatusValidation, CodeInvalidInput, "Check your payment details and try again."
	case errors.Is(err, ErrCheckoutUnavailable), errors.Is(err, ErrProviderNotConfigured):
		return StatusUnavailable, CodeUnavailable, "Payments are temporarily unavailable. Try again shortly."
	default:
	}

	var v validationError
	if errors.As(err, &v) {
		switch string(v) {
		case "amount is required", "amount must be positive", "amount_micro must be greater than zero":
			return StatusValidation, CodeInvalidAmount, "Enter a valid payment amount."
		case "invalid customer id", "customer not found":
			return StatusNotFound, CodeInvalidCustomer, "Account not found."
		case "invalid request body", "invalid intent id", "idempotency_key is required":
			return StatusValidation, CodeInvalidInput, "Check your payment details and try again."
		}
	}

	msg := err.Error()
	if strings.Contains(msg, "forbidden") || strings.Contains(msg, "unauthorized") {
		return StatusForbidden, CodeForbidden, "You are not allowed to perform this payment."
	}

	return StatusFailed, CodeFailed, "Something went wrong processing your payment. Try again or contact support."
}

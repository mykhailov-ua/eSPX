package payment

// Payment-specific HTTP status codes in the 460-470 range so HTMX can branch on validation
// without treating fixable input as a 500 from the host page middleware.
const (
	StatusValidation  = 460 // fixable input: amount, currency, missing fields
	StatusConflict    = 461 // idempotency or duplicate intent mismatch
	StatusUnavailable = 462 // checkout provider temporarily down
	StatusNotFound    = 463 // intent or customer not found
	StatusForbidden   = 464 // session, CSRF, or ownership
	StatusFailed      = 470 // sanitized operational failure; real error logged server-side
)

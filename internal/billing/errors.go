package billing

import "errors"

var (
	// ErrCustomerNotFound is returned when the ads.customers row does not exist.
	ErrCustomerNotFound = errors.New("customer not found")
	// ErrInvalidCustomerID is returned for malformed UUID input.
	ErrInvalidCustomerID = errors.New("invalid customer id")
	// ErrInvalidInvoiceID is returned for malformed invoice UUID input.
	ErrInvalidInvoiceID = errors.New("invalid invoice id")
	// ErrInvalidBillingMonth is returned when the billing period is not the first day of a month.
	ErrInvalidBillingMonth = errors.New("billing_month must be the first day of a calendar month")
	// ErrInvoiceNotFound is returned when no invoice row exists for the requested id.
	ErrInvoiceNotFound = errors.New("invoice not found")
	// ErrLedgerDrift is returned when customer balance does not match the ledger sum.
	ErrLedgerDrift = errors.New("ledger balance drift detected")
)

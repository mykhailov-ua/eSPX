package payment

import (
	"time"

	"espx/internal/payment/db"

	"github.com/google/uuid"
)

// FinancialReconSummary aggregates counters produced by a reconciliation run.
type FinancialReconSummary struct {
	RunID            int64
	PeriodStart      time.Time
	PeriodEnd        time.Time
	IntentsChecked   int
	FindingsCount    int
	FindingsByKind   map[string]int
	TopupAligned     int
	TopupMissing     int
	DeadOutboxRows   int
	SettlementFailed int
}

// FinancialReconFinding is an in-memory finding before persistence.
type FinancialReconFinding struct {
	Kind               db.PaymentFinancialFindingKind
	PaymentIntentID    uuid.UUID
	CustomerID         uuid.UUID
	PaymentAmountMicro int64
	LedgerAmountMicro  int64
	DeltaMicro         int64
	Detail             map[string]any
}

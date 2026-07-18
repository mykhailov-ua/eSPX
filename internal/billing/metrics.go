package billing

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	InvoicesGeneratedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "billing_invoices_generated_total",
		Help: "Invoices generated successfully",
	})

	InvoiceErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "billing_invoice_errors_total",
		Help: "Invoice generation failures by reason",
	}, []string{"reason"})

	LedgerDriftTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "billing_ledger_drift_total",
		Help: "Customer balance vs ledger sum drift detected",
	})

	LedgerInvariantFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "billing_ledger_invariant_failures_total",
		Help: "Ledger invariant check failures detected",
	})
)

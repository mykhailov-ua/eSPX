package payment

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// IntentsTotal tracks intent volume by terminal status for checkout funnel and failure rate dashboards.
	IntentsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payment_intents_total",
		Help: "Payment intents created or transitioned by status",
	}, []string{"status"})

	// WebhookEventsTotal separates processed, duplicate, and ignored Stripe events for dedup health.
	WebhookEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payment_webhook_events_total",
		Help: "Stripe webhook events processed by outcome",
	}, []string{"outcome"})

	// OutboxPending surfaces settlement backlog when webhooks outpace management ledger writes.
	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "payment_outbox_pending",
		Help: "Payment outbox rows awaiting settlement",
	})

	// SettlementErrorsTotal counts outbox delivery failures distinct from webhook processing errors.
	SettlementErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payment_settlement_errors_total",
		Help: "Failed settlement attempts from the payment outbox worker",
	})

	// WebhookSignatureFailuresTotal isolates misconfigured secrets or replay attacks from business logic errors.
	WebhookSignatureFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payment_webhook_signature_failures_total",
		Help: "Rejected Stripe webhook signatures",
	})
)

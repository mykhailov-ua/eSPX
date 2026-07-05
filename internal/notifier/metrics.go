package notifier

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	queuePendingTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "notifier_queue_pending_total",
		Help: "Count of notifications in PENDING status awaiting delivery",
	})

	queueOldestPendingSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "notifier_queue_oldest_pending_seconds",
		Help: "Age in seconds of the oldest PENDING notification (0 when queue empty)",
	})

	queueProcessingTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "notifier_queue_processing_total",
		Help: "Count of notifications in PROCESSING status (claimed by a worker)",
	})

	queueOldestProcessingSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "notifier_queue_oldest_processing_seconds",
		Help: "Age in seconds of the oldest PROCESSING claim (0 when none in flight)",
	})

	deliveryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notifier_delivery_total",
		Help: "Notification delivery attempts by provider and result",
	}, []string{"provider", "result"})

	deliveryDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notifier_delivery_duration_seconds",
		Help:    "Wall time spent in provider Send calls",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"provider"})

	circuitBreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "notifier_circuit_breaker_state",
		Help: "Circuit breaker state per provider: 0=closed, 1=open, 2=half-open",
	}, []string{"provider"})

	dedupAggregatedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "notifier_dedup_aggregated_total",
		Help: "Notifications merged into deduplicated delivery groups",
	})

	fallbackTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notifier_fallback_total",
		Help: "Fallback delivery attempts after primary provider failure",
	}, []string{"from_provider", "to_provider"})

	broadcastPartialTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "notifier_broadcast_partial_total",
		Help: "Broadcast deliveries that succeeded on a quorum but had channel failures",
	})

	permanentFailuresTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "notifier_permanent_failures_total",
		Help: "Notifications marked FAILED after exhausting delivery retries",
	})

	workerBatchProcessed = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "notifier_worker_batch_processed",
		Help:    "Notifications marked processed per worker iteration",
		Buckets: []float64{0, 1, 2, 5, 10, 20, 50, 100},
	})

	workerIterationErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "notifier_worker_iteration_errors_total",
		Help: "Worker polling iterations that failed before commit",
	})

	retentionDeletedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notifier_retention_deleted_total",
		Help: "Notifications deleted by the retention janitor",
	}, []string{"status"})
)

// RegisterMetrics registers notifier Prometheus collectors.
func RegisterMetrics() {
	prometheus.MustRegister(
		queuePendingTotal,
		queueOldestPendingSeconds,
		queueProcessingTotal,
		queueOldestProcessingSeconds,
		deliveryTotal,
		deliveryDurationSeconds,
		circuitBreakerState,
		dedupAggregatedTotal,
		fallbackTotal,
		broadcastPartialTotal,
		permanentFailuresTotal,
		workerBatchProcessed,
		workerIterationErrorsTotal,
		retentionDeletedTotal,
	)
}

func recordDelivery(provider string, succeeded bool, durationSeconds float64) {
	result := "failed"
	if succeeded {
		result = "sent"
	}
	deliveryTotal.WithLabelValues(provider, result).Inc()
	deliveryDurationSeconds.WithLabelValues(provider).Observe(durationSeconds)
}

func recordCircuitBreakerState(provider string, state CircuitState) {
	circuitBreakerState.WithLabelValues(provider).Set(float64(state))
}

func recordFallback(fromProvider, toProvider string) {
	fallbackTotal.WithLabelValues(fromProvider, toProvider).Inc()
}

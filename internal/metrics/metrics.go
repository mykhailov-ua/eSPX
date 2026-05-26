package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTP Metrics
	HttpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_http_requests_total",
		Help: "Total number of HTTP requests by status code",
	}, []string{"method", "path", "status"})

	HttpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_http_request_duration_seconds",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	// Event Processing Metrics
	EventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_events_processed_total",
		Help: "Total number of events successfully accepted into Redis Streams",
	})

	EventsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_events_dropped_total",
		Help: "Total number of events dropped due to Redis ingestion failure",
	})

	FilterBlockedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_filter_blocked_total",
		Help: "Total number of events blocked by filters",
	}, []string{"reason"})

	// DB Metrics
	DbWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_db_write_duration_seconds",
		Help:    "Duration of database batch write operations",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"type"})

	DbWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_db_write_errors_total",
		Help: "Total number of database write errors",
	}, []string{"type"})

	// System Resilience Metrics
	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_circuit_breaker_state",
		Help: "Current state of the circuit breaker (0=closed, 1=open, 2=half-open)",
	}, []string{"group"})

	DlqSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_dlq_size_total",
		Help: "Current number of events in the Dead Letter Queue",
	})

	// Management & Financial Metrics
	CommissionsCollectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_management_commissions_total",
		Help: "Total amount of commissions collected from campaign cancellations",
	})

	BalanceTopupsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_management_topups_total",
		Help: "Total amount of customer balance top-ups",
	}, []string{"currency"})

	ActiveCampaigns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_management_active_campaigns_count",
		Help: "Current number of active campaigns in the system",
	})

	DataDriftRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_reconciliation_drift_ratio",
		Help: "Ratio of discrepancy between Postgres and ClickHouse spend",
	}, []string{"campaign_id"})
)

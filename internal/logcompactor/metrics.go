package logcompactor

import "github.com/prometheus/client_golang/prometheus"

var (
	hotLagSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_compactor_hot_lag_seconds",
			Help: "Age of the oldest uncompacted hot-tier segment.",
		},
	)

	coldLagSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_compactor_cold_lag_seconds",
			Help: "Age of the oldest warm-tier segment pending cold rollup.",
		},
	)

	leaderHeld = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_compactor_leader",
			Help: "1 when this instance holds the compactor leader lock, else 0.",
		},
	)

	hotPendingTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_compactor_hot_pending_total",
			Help: "Hot-tier segments waiting for compaction.",
		},
	)

	warmPendingTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "log_compactor_warm_pending_total",
			Help: "Warm-tier segments waiting for cold rollup.",
		},
	)

	coldRollupsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_compactor_cold_rollups_total",
			Help: "Warm segments successfully rolled up into ClickHouse.",
		},
	)

	coldRollupRowsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_compactor_cold_rollup_rows_total",
			Help: "Hourly rollup rows inserted into ClickHouse.",
		},
	)

	coldRollupErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_compactor_cold_errors_total",
			Help: "Cold-tier rollup failures.",
		},
	)

	coldListErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_compactor_cold_list_errors_total",
			Help: "Failures listing warm-tier segments for cold rollup.",
		},
	)

	segmentsCompactedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_compactor_segments_compacted_total",
			Help: "Hot segments successfully compacted into warm tier.",
		},
	)

	segmentsCompactErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_compactor_errors_total",
			Help: "Compaction failures during hot segment processing.",
		},
	)

	segmentsListedErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "log_compactor_list_errors_total",
			Help: "Failures listing hot-tier segments.",
		},
	)

	recordsKeptRatio = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "log_compactor_records_kept_ratio",
			Help:    "Ratio of kept records to original records per segment.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
	)

	compressionRatio = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "log_compactor_plaintext_ratio",
			Help:    "Ratio of filtered plaintext size to original plaintext size.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
	)
)

// RegisterMetrics exposes compactor counters for ops dashboards.
func RegisterMetrics() {
	prometheus.MustRegister(
		hotLagSeconds,
		coldLagSeconds,
		leaderHeld,
		hotPendingTotal,
		warmPendingTotal,
		coldRollupsTotal,
		coldRollupRowsTotal,
		coldRollupErrors,
		coldListErrors,
		segmentsCompactedTotal,
		segmentsCompactErrors,
		segmentsListedErrors,
		recordsKeptRatio,
		compressionRatio,
	)
}

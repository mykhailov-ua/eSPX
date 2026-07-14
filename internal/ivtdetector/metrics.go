package ivtdetector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ivtCandidatesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ivt_candidates_total",
		Help: "Suspicious IP candidates discovered per detection rule",
	}, []string{"rule"})

	ivtEnqueuedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ivt_enqueued_total",
		Help: "Blacklist enqueue operations completed by the IVT detector",
	})

	ivtBackpressureDropsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ivt_backpressure_drops_total",
		Help: "Detector cycles skipped due to management outbox backpressure",
	})

	mlScoringDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ml_scoring_duration_seconds",
		Help:    "Duration of ML scoring in seconds",
		Buckets: prometheus.DefBuckets,
	})

	mlCandidatesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ml_candidates_total",
		Help: "Total number of candidates scored by ML",
	})

	mlScoringErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ml_scoring_errors_total",
		Help: "Total number of ML scoring errors",
	})

	mlEnforcementEnqueuedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ml_enforcement_enqueued_total",
		Help: "Total number of ML enforcement threats enqueued",
	}, []string{"action"})
)

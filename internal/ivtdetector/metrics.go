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

	fraudScoringDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "fraud_scoring_duration_seconds",
		Help:    "Duration of ML scoring in seconds",
		Buckets: prometheus.DefBuckets,
	})

	fraudScoringCandidatesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fraud_scoring_candidates_total",
		Help: "Total number of candidates scored by ML",
	})

	fraudScoringErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fraud_scoring_errors_total",
		Help: "Total number of ML scoring errors",
	})

	fraudEnforcementEnqueuedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fraud_enforcement_enqueued_total",
		Help: "Total number of ML enforcement threats enqueued",
	}, []string{"action"})
)

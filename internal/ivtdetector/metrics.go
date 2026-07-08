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
)

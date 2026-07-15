package ingestion

import (
	"espx/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// preboundFraudMetrics holds pre-resolved fraud telemetry counters for the filter hot path.
type preboundFraudMetrics struct {
	tierPass    prometheus.Counter
	tierSuspect prometheus.Counter
	tierIVT     prometheus.Counter
	tierBlock   prometheus.Counter

	reason [fraudReasonCount]prometheus.Counter

	l1Reject prometheus.Counter
}

var boundFraudMetrics = newPreboundFraudMetrics()

func newPreboundFraudMetrics() preboundFraudMetrics {
	pm := preboundFraudMetrics{
		tierPass:    metrics.FraudTierTotal.WithLabelValues("pass"),
		tierSuspect: metrics.FraudTierTotal.WithLabelValues("suspect"),
		tierIVT:     metrics.FraudTierTotal.WithLabelValues("ivt"),
		tierBlock:   metrics.FraudTierTotal.WithLabelValues("block"),
		l1Reject:    metrics.L1RejectTotal,
	}
	for id := FraudReasonID(1); id < fraudReasonCount; id++ {
		code := FraudReasonCode(id)
		if code != "" {
			pm.reason[id] = metrics.FraudReasonTotal.WithLabelValues(code)
		}
	}
	return pm
}

func (pm *preboundFraudMetrics) tierCounter(tier FraudTier) prometheus.Counter {
	switch tier {
	case FraudTierSuspect:
		return pm.tierSuspect
	case FraudTierIVT:
		return pm.tierIVT
	case FraudTierBlock:
		return pm.tierBlock
	default:
		return pm.tierPass
	}
}

// recordFraudMetrics emits pre-bound fraud telemetry after score accumulation.
func recordFraudMetrics(acc *fraudAccumulator, tier FraudTier, layer FraudLayer) {
	if acc == nil || acc.count == 0 {
		return
	}
	metrics.FraudScoreHistogram.Observe(float64(acc.score))
	boundFraudMetrics.tierCounter(tier).Inc()
	for i := uint8(0); i < acc.count; i++ {
		id := acc.signals[i]
		if id > FraudReasonNone && id < fraudReasonCount {
			if c := boundFraudMetrics.reason[id]; c != nil {
				c.Inc()
			}
		}
	}
	if layer == FraudLayerL1Reject {
		boundFraudMetrics.l1Reject.Inc()
	}
}

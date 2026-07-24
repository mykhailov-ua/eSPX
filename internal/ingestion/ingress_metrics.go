package ingestion

import "espx/internal/metrics"

func incIngressLegacyJSON() {
	metrics.IngressLegacyJSONTotal.Inc()
}

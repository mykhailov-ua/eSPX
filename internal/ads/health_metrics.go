package ads

import (
	"strconv"

	"espx/internal/metrics"
)

// exportHealthProbeMetrics mirrors gnet /health atomics into Prometheus gauges.
func exportHealthProbeMetrics(healthy bool, shardHealthy []int32) {
	if healthy {
		metrics.TrackerHealthDegraded.Set(0)
	} else {
		metrics.TrackerHealthDegraded.Set(1)
	}
	for i, st := range shardHealthy {
		v := 0.0
		if st == 1 {
			v = 1
		}
		metrics.TrackerRedisShardHealthy.WithLabelValues(strconv.Itoa(i)).Set(v)
	}
}

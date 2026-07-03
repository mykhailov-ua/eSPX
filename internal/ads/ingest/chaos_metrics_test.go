package ingest

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/metrics"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

const chaosRedisShardLabel = "0"

// chaosGaugeValue reads a Prometheus gauge for CHAOS.md §7 metric assertions.
func chaosGaugeValue(t *testing.T, g interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, g.Write(&m))
	return m.GetGauge().GetValue()
}

func trackerHealthDegradedMetric(t *testing.T) float64 {
	t.Helper()
	return chaosGaugeValue(t, metrics.TrackerHealthDegraded)
}

func redisBreakerStateMetric(t *testing.T, shard string) float64 {
	t.Helper()
	g, err := metrics.RedisBreakerState.GetMetricWithLabelValues(shard)
	require.NoError(t, err)
	return chaosGaugeValue(t, g)
}

// requireRedisOutageMetrics waits until CHAOS.md §7 signals Redis degradation:
// ad_tracker_health_degraded==1 (health probe) or ad_redis_breaker_state==open.
func requireRedisOutageMetrics(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		if trackerHealthDegradedMetric(t) == 1 {
			return true
		}
		return redisBreakerStateMetric(t, chaosRedisShardLabel) == float64(database.CircuitOpen)
	}, 15*time.Second, 200*time.Millisecond,
		"during Redis outage expect ad_tracker_health_degraded=1 or ad_redis_breaker_state{shard=%q}=open",
		chaosRedisShardLabel)
}

// requireRedisSteadyStateMetrics proves steady-state restoration after recovery:
// ad_tracker_health_degraded returns to 0 and ad_redis_breaker_state{shard} is closed.
func requireRedisSteadyStateMetrics(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		if trackerHealthDegradedMetric(t) != 0 {
			return false
		}
		return redisBreakerStateMetric(t, chaosRedisShardLabel) == float64(database.CircuitClosed)
	}, 30*time.Second, 200*time.Millisecond,
		"steady-state restoration: ad_tracker_health_degraded=0 and ad_redis_breaker_state{shard=%q}=closed",
		chaosRedisShardLabel)
}

func tripChaosRedisBreaker(t *testing.T, infra *adsChaosInfra) {
	t.Helper()
	require.NotNil(t, infra.RedisBreaker)
	ctx := context.Background()
	require.Eventually(t, func() bool {
		for range 3 {
			_ = infra.Redis.Ping(ctx).Err()
		}
		return infra.RedisBreaker.State() == database.CircuitOpen
	}, 10*time.Second, 50*time.Millisecond)
}

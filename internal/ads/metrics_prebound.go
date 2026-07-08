package ads

import (
	"strconv"

	"espx/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// preboundTrackMetrics holds pre-resolved Prometheus counters for the gnet track handler.
type preboundTrackMetrics struct {
	throughputProto prometheus.Counter
	throughputJSON  prometheus.Counter

	decisionAccepted         prometheus.Counter
	decisionEmergencyBreaker prometheus.Counter
	decisionRateLimited      prometheus.Counter
	decisionDuplicate        prometheus.Counter
	decisionBudgetExhausted  prometheus.Counter
	decisionPacingLimit      prometheus.Counter
	decisionFrequencyCapped  prometheus.Counter
	decisionGeoBlocked       prometheus.Counter
	decisionScheduleBlocked  prometheus.Counter
	decisionCampaignNotFound prometheus.Counter
	decisionBidFloor         prometheus.Counter
	decisionFilterTimeout    prometheus.Counter
	decisionFraud            prometheus.Counter
	decisionConsentDenied    prometheus.Counter
	decisionInfraUnavailable prometheus.Counter

	blockedEmergencyBreaker prometheus.Counter
	blockedRateLimit        prometheus.Counter
	blockedDuplicate        prometheus.Counter
	blockedBudget           prometheus.Counter
	blockedPacing           prometheus.Counter
	blockedFreq             prometheus.Counter
	blockedGeo              prometheus.Counter
	blockedSchedule         prometheus.Counter
	blockedCampaignNotFound prometheus.Counter
	blockedBidFloor         prometheus.Counter
	blockedFilterTimeout    prometheus.Counter
	blockedFraud            prometheus.Counter
	blockedConsent          prometheus.Counter
	blockedInfra            prometheus.Counter
}

// newPreboundTrackMetrics binds all track-path label values once at handler startup.
func newPreboundTrackMetrics() preboundTrackMetrics {
	return preboundTrackMetrics{
		throughputProto: metrics.FilterThroughput.WithLabelValues("protobuf"),
		throughputJSON:  metrics.FilterThroughput.WithLabelValues("json"),

		decisionAccepted:         metrics.FilterDecisions.WithLabelValues("accepted"),
		decisionEmergencyBreaker: metrics.FilterDecisions.WithLabelValues("emergency_breaker"),
		decisionRateLimited:      metrics.FilterDecisions.WithLabelValues("rate_limited"),
		decisionDuplicate:        metrics.FilterDecisions.WithLabelValues("duplicate"),
		decisionBudgetExhausted:  metrics.FilterDecisions.WithLabelValues("budget_exhausted"),
		decisionPacingLimit:      metrics.FilterDecisions.WithLabelValues("pacing_limit"),
		decisionFrequencyCapped:  metrics.FilterDecisions.WithLabelValues("frequency_capped"),
		decisionGeoBlocked:       metrics.FilterDecisions.WithLabelValues("geo_blocked"),
		decisionScheduleBlocked:  metrics.FilterDecisions.WithLabelValues("schedule_blocked"),
		decisionCampaignNotFound: metrics.FilterDecisions.WithLabelValues("campaign_not_found"),
		decisionBidFloor:         metrics.FilterDecisions.WithLabelValues("bid_floor"),
		decisionFilterTimeout:    metrics.FilterDecisions.WithLabelValues("filter_timeout"),
		decisionFraud:            metrics.FilterDecisions.WithLabelValues("fraud"),
		decisionConsentDenied:    metrics.FilterDecisions.WithLabelValues("consent_denied"),
		decisionInfraUnavailable: metrics.FilterDecisions.WithLabelValues("infra_unavailable"),

		blockedEmergencyBreaker: metrics.FilterBlockedTotal.WithLabelValues("emergency_breaker"),
		blockedRateLimit:        metrics.FilterBlockedTotal.WithLabelValues("rate_limit"),
		blockedDuplicate:        metrics.FilterBlockedTotal.WithLabelValues("duplicate"),
		blockedBudget:           metrics.FilterBlockedTotal.WithLabelValues("budget"),
		blockedPacing:           metrics.FilterBlockedTotal.WithLabelValues("pacing"),
		blockedFreq:             metrics.FilterBlockedTotal.WithLabelValues("freq"),
		blockedGeo:              metrics.FilterBlockedTotal.WithLabelValues("geo"),
		blockedSchedule:         metrics.FilterBlockedTotal.WithLabelValues("schedule"),
		blockedCampaignNotFound: metrics.FilterBlockedTotal.WithLabelValues("campaign_not_found"),
		blockedBidFloor:         metrics.FilterBlockedTotal.WithLabelValues("bid_floor"),
		blockedFilterTimeout:    metrics.FilterBlockedTotal.WithLabelValues("filter_timeout"),
		blockedFraud:            metrics.FilterBlockedTotal.WithLabelValues("fraud"),
		blockedConsent:          metrics.FilterBlockedTotal.WithLabelValues("consent_denied"),
		blockedInfra:            metrics.FilterBlockedTotal.WithLabelValues("infra_unavailable"),
	}
}

// newRedisLuaObservers pre-binds per-shard Lua latency histogram observers.
func newRedisLuaObservers(numShards int) []prometheus.Observer {
	if numShards <= 0 {
		numShards = 1
	}
	observers := make([]prometheus.Observer, numShards)
	for i := range observers {
		observers[i] = metrics.RedisLuaDuration.WithLabelValues(strconv.Itoa(i))
	}
	return observers
}

// newRedisLuaNoScriptCounters pre-binds per-shard NOSCRIPT fallback counters.
func newRedisLuaNoScriptCounters(numShards int) []prometheus.Counter {
	if numShards <= 0 {
		numShards = 1
	}
	counters := make([]prometheus.Counter, numShards)
	for i := range counters {
		counters[i] = metrics.RedisLuaNoScriptTotal.WithLabelValues(strconv.Itoa(i))
	}
	return counters
}

// incRedisLuaNoScript records a NOSCRIPT fallback on the pre-bound shard counter when available.
func incRedisLuaNoScript(counters []prometheus.Counter, shard int) {
	if shard >= 0 && shard < len(counters) {
		counters[shard].Inc()
		return
	}
	metrics.RedisLuaNoScriptTotal.WithLabelValues(strconv.Itoa(shard)).Inc()
}

// observeRedisLua records Lua duration on the pre-bound shard observer when available.
func observeRedisLua(observers []prometheus.Observer, shard int, seconds float64) {
	if shard >= 0 && shard < len(observers) {
		observers[shard].Observe(seconds)
		return
	}
	metrics.RedisLuaDuration.WithLabelValues(strconv.Itoa(shard)).Observe(seconds)
}

// newRedisOpsCounters pre-binds per-shard unified-filter Redis op counters.
func newRedisOpsCounters(numShards int) []prometheus.Counter {
	if numShards <= 0 {
		numShards = 1
	}
	counters := make([]prometheus.Counter, numShards)
	for i := range counters {
		counters[i] = metrics.RedisOpsTotal.WithLabelValues(strconv.Itoa(i))
	}
	return counters
}

// incRedisOps records one EvalSha round trip on the pre-bound shard counter when available.
func incRedisOps(counters []prometheus.Counter, shard int) {
	if shard >= 0 && shard < len(counters) {
		counters[shard].Inc()
		return
	}
	metrics.RedisOpsTotal.WithLabelValues(strconv.Itoa(shard)).Inc()
}

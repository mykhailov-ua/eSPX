package ads

import (
	"strconv"
	"sync/atomic"

	"espx/internal/metrics"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

// Pre-bound filter error counters avoid Prometheus label lookup on the rejection hot path.
var (
	filterGeoLookupErrors        = metrics.FilterInternalErrors.WithLabelValues("geo_lookup")
	filterFraudStreamWriteErrors = metrics.FilterInternalErrors.WithLabelValues("fraud_stream_write")
	filterEngineFailures         = metrics.FilterInternalErrors.WithLabelValues("filter_engine")
	filterGeoDuration            = metrics.FilterGeoDuration
	geoMetricsSeq                atomic.Uint64
)

const sampledCampaignBuckets = 256

var sampledCampaignBucketLabels [sampledCampaignBuckets]string

func init() {
	for i := range sampledCampaignBucketLabels {
		sampledCampaignBucketLabels[i] = strconv.Itoa(i)
	}
}

// redisShardObservability holds Phase-0 hot-shard metrics: per-shard ops plus sampled campaign breakdown.
type redisShardObservability struct {
	opsCounters             []prometheus.Counter
	sampleMask              uint64
	sampledCampaignCounters [][]prometheus.Counter
	sampledSpendCounters    [][]prometheus.Counter
}

// newRedisShardObservability pre-binds per-shard Redis op counters and configures campaign sampling.
func newRedisShardObservability(numShards int, sampleMask uint64) redisShardObservability {
	if numShards <= 0 {
		numShards = 1
	}
	if sampleMask == 0 {
		sampleMask = luaMetricsSampleMask
	}
	o := redisShardObservability{
		opsCounters:             newRedisOpsCounters(numShards),
		sampleMask:              sampleMask,
		sampledCampaignCounters: make([][]prometheus.Counter, numShards),
		sampledSpendCounters:    make([][]prometheus.Counter, numShards),
	}
	shardLabel := make([]string, numShards)
	for s := 0; s < numShards; s++ {
		shardLabel[s] = strconv.Itoa(s)
		o.sampledCampaignCounters[s] = make([]prometheus.Counter, sampledCampaignBuckets)
		o.sampledSpendCounters[s] = make([]prometheus.Counter, sampledCampaignBuckets)
		for b := 0; b < sampledCampaignBuckets; b++ {
			o.sampledCampaignCounters[s][b] = metrics.RedisCampaignOpsSampledTotal.WithLabelValues(shardLabel[s], sampledCampaignBucketLabels[b])
			o.sampledSpendCounters[s][b] = metrics.TrackerCampaignSpendSampledTotal.WithLabelValues(shardLabel[s], sampledCampaignBucketLabels[b])
		}
	}
	return o
}

// recordLuaOp increments per-shard Redis RPS and, on sample ticks, per-campaign op counters.
func (o *redisShardObservability) recordLuaOp(shard int, campaignID uuid.UUID, sample bool) {
	incRedisOps(o.opsCounters, shard)
	if sample {
		recordSampledCampaignOp(o, shard, campaignID)
	}
}

// recordAcceptedSpend adds sampled micro-unit spend for accepted events that debited budget.
func (o *redisShardObservability) recordAcceptedSpend(shard int, campaignID uuid.UUID, spendMicro int64, sample bool) {
	if !sample || spendMicro <= 0 {
		return
	}
	recordSampledCampaignSpend(o, shard, campaignID, spendMicro)
}

func sampledCampaignBucket(campaignID uuid.UUID) int {
	return int(campaignID[0]) ^ int(campaignID[15])
}

// recordSampledCampaignOp emits a downsampled per-shard campaign-bucket op counter for Grafana top-N.
func recordSampledCampaignOp(o *redisShardObservability, shard int, campaignID uuid.UUID) {
	if len(o.sampledCampaignCounters) == 0 {
		return
	}
	if shard < 0 {
		shard = 0
	}
	if shard >= len(o.sampledCampaignCounters) {
		shard = shard % len(o.sampledCampaignCounters)
	}
	bucket := sampledCampaignBucket(campaignID)
	o.sampledCampaignCounters[shard][bucket].Inc()
}

// recordSampledCampaignSpend emits downsampled accepted spend for hot-campaign dashboards.
func recordSampledCampaignSpend(o *redisShardObservability, shard int, campaignID uuid.UUID, spendMicro int64) {
	if len(o.sampledSpendCounters) == 0 {
		return
	}
	if shard < 0 {
		shard = 0
	}
	if shard >= len(o.sampledSpendCounters) {
		shard = shard % len(o.sampledSpendCounters)
	}
	bucket := sampledCampaignBucket(campaignID)
	o.sampledSpendCounters[shard][bucket].Add(float64(spendMicro))
}

package filter

import (
	"context"
	"testing"

	"espx/internal/domain"
	"espx/internal/metrics"

	adstest "espx/internal/ads/testutil"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// Guards geo lookup failures increment telemetry without blocking the event.
func TestGeoFilter_lookupErrorIncrementsCounter(t *testing.T) {
	before := testutil.ToFloat64(filterGeoLookupErrors)
	campID := uuid.New()
	adstest.CachedMockCamp.Store(&domain.Campaign{
		ID:              campID,
		TargetCountries: map[string]struct{}{"US": {}},
	})
	t.Cleanup(func() { adstest.CachedMockCamp.Store(nil) })

	f := NewGeoFilter(errGeoProvider{}, &adstest.MockRegistry{})
	err := f.Check(context.Background(), &domain.Event{IP: "8.8.8.8", CampaignID: campID})
	require.NoError(t, err)
	require.Equal(t, before+1, testutil.ToFloat64(filterGeoLookupErrors))
}

func TestRedisShardObservability_recordLuaOp(t *testing.T) {
	t.Parallel()

	campaignID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	obs := newRedisShardObservability(4, 0) // sample every op
	bucket := sampledCampaignBucketLabels[sampledCampaignBucket(campaignID)]

	beforeShard0 := testutil.ToFloat64(metrics.RedisOpsTotal.WithLabelValues("0"))
	beforeCamp := testutil.ToFloat64(metrics.RedisCampaignOpsSampledTotal.WithLabelValues("0", bucket))

	obs.recordLuaOp(0, campaignID, true)

	require.Equal(t, beforeShard0+1, testutil.ToFloat64(metrics.RedisOpsTotal.WithLabelValues("0")))
	require.Equal(t, beforeCamp+1, testutil.ToFloat64(metrics.RedisCampaignOpsSampledTotal.WithLabelValues("0", bucket)))

	beforeShard0NoSample := testutil.ToFloat64(metrics.RedisOpsTotal.WithLabelValues("0"))
	beforeCampNoSample := testutil.ToFloat64(metrics.RedisCampaignOpsSampledTotal.WithLabelValues("0", bucket))
	obs.recordLuaOp(0, campaignID, false)

	require.Equal(t, beforeShard0NoSample+1, testutil.ToFloat64(metrics.RedisOpsTotal.WithLabelValues("0")))
	require.Equal(t, beforeCampNoSample, testutil.ToFloat64(metrics.RedisCampaignOpsSampledTotal.WithLabelValues("0", bucket)))
}

func TestRedisShardObservability_recordAcceptedSpend(t *testing.T) {
	t.Parallel()

	campaignID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	obs := newRedisShardObservability(4, 0)
	bucket := sampledCampaignBucketLabels[sampledCampaignBucket(campaignID)]

	key := metrics.TrackerCampaignSpendSampledTotal.WithLabelValues("1", bucket)
	before := testutil.ToFloat64(key)

	obs.recordAcceptedSpend(1, campaignID, 1_500_000, true)
	require.InDelta(t, before+1_500_000, testutil.ToFloat64(key), 0.001)

	obs.recordAcceptedSpend(1, campaignID, 1_500_000, false)
	require.InDelta(t, before+1_500_000, testutil.ToFloat64(key), 0.001)
}

func TestSpendMicroFromAny(t *testing.T) {
	t.Parallel()
	require.Equal(t, int64(42), spendMicroFromAny(int64(42)))
	require.Equal(t, int64(0), spendMicroFromAny("nope"))
}

func TestNewRedisShardObservability_defaultSampleMask(t *testing.T) {
	t.Parallel()
	obs := newRedisShardObservability(4, 0)
	require.Equal(t, luaMetricsSampleMask, obs.sampleMask)
}

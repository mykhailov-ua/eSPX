package ingestion

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMonoElapsedSeconds_nonNegative(t *testing.T) {
	start := monotonicNano()
	elapsed := monoElapsedSeconds(start)
	assert.GreaterOrEqual(t, elapsed, 0.0)
}

func TestShouldSampleLuaMetrics_rate(t *testing.T) {
	var sampled int
	const n = 128 * 100
	for i := uint64(1); i <= n; i++ {
		if shouldSampleLuaMetrics(i) {
			sampled++
		}
	}
	// 1/(mask+1) with mask=127 => 1/128
	want := n / 128
	assert.InDelta(t, want, sampled, 2)
}

func TestLuaMetricsSampleMask_constant(t *testing.T) {
	assert.Equal(t, uint64(127), luaMetricsSampleMask)
}

func BenchmarkMonoElapsedSeconds(b *testing.B) {
	start := monotonicNano()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = monoElapsedSeconds(start)
	}
}

func BenchmarkTimeSince_equivalent(b *testing.B) {
	start := monotonicNano()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = monoElapsedSeconds(start)
	}
}

func TestShouldSampleHistogram_maskZeroObservesAll(t *testing.T) {
	for i := uint64(1); i < 50; i++ {
		assert.True(t, shouldSampleHistogram(i, 0))
	}
}

func TestHistogramSampleMaskFromConfig_negativeMeansAll(t *testing.T) {
	assert.Equal(t, uint64(0), histogramSampleMaskFromConfig(-1))
	assert.Equal(t, uint64(127), histogramSampleMaskFromConfig(0))
	assert.Equal(t, uint64(127), histogramSampleMaskFromConfig(127))
}

func TestObserveHistogramSampled_respectsMask(t *testing.T) {
	spy := &spyObserver{}
	var seq atomic.Uint64
	start := monotonicNano()

	const n = 256
	for i := 0; i < n; i++ {
		observeHistogramSampled(&seq, 127, spy, start)
	}
	assert.InDelta(t, n/128, spy.count(), 2)
}

func TestObserveHistogramSampled_maskZeroAlwaysObserves(t *testing.T) {
	spy := &spyObserver{}
	var seq atomic.Uint64
	start := monotonicNano()

	for i := 0; i < 10; i++ {
		observeHistogramSampled(&seq, 0, spy, start)
	}
	assert.Equal(t, 10, spy.count())
}

func BenchmarkObserveHistogramSampled_sampled(b *testing.B) {
	spy := &spyObserver{}
	var seq atomic.Uint64
	start := monotonicNano()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		observeHistogramSampled(&seq, 127, spy, start)
	}
}

func BenchmarkObserveHistogramSampled_always(b *testing.B) {
	spy := &spyObserver{}
	var seq atomic.Uint64
	start := monotonicNano()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		observeHistogramSampled(&seq, 0, spy, start)
	}
}

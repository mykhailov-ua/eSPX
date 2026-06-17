package ads

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Guards monotonic elapsed seconds never go negative after clock adjustments.
func TestMonoElapsedSeconds_nonNegative(t *testing.T) {
	start := monotonicNano()
	elapsed := monoElapsedSeconds(start)
	assert.GreaterOrEqual(t, elapsed, 0.0)
}

// Guards event-loop work duration pattern matches monotonic measurement contract.
func TestGnetEventLoopWorkDuration_pattern(t *testing.T) {
	start := monotonicNano()
	elapsed := monoElapsedSeconds(start)
	assert.GreaterOrEqual(t, elapsed, 0.0)
	assert.Less(t, elapsed, 1.0)
}

// Guards Lua metrics sampling rate matches configured mask probability.
func TestShouldSampleLuaMetrics_rate(t *testing.T) {
	var sampled int
	const n = 128 * 100
	for i := uint64(1); i <= n; i++ {
		if shouldSampleLuaMetrics(i) {
			sampled++
		}
	}

	want := n / 128
	assert.InDelta(t, want, sampled, 2)
}

// Tracks monotonic elapsed helper cost on the hot path.
func BenchmarkMonoElapsedSeconds(b *testing.B) {
	start := monotonicNano()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = monoElapsedSeconds(start)
	}
}

// Tracks wall-clock time.Since baseline against monotonic elapsed for syscall comparison.
func BenchmarkTimeSince_wallClock(b *testing.B) {
	start := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Since(start).Seconds()
	}
}

// Guards zero histogram mask observes every sample for full fidelity.
func TestShouldSampleHistogram_maskZeroObservesAll(t *testing.T) {
	for i := uint64(1); i < 50; i++ {
		assert.True(t, shouldSampleHistogram(i, 0))
	}
}

// Guards negative histogram sample config means observe all samples.
func TestHistogramSampleMaskFromConfig_negativeMeansAll(t *testing.T) {
	assert.Equal(t, uint64(0), histogramSampleMaskFromConfig(-1))
	assert.Equal(t, uint64(127), histogramSampleMaskFromConfig(0))
	assert.Equal(t, uint64(127), histogramSampleMaskFromConfig(127))
}

// Guards histogram sampling respects mask and skips non-selected samples.
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

// Guards zero mask always records histogram observations.
func TestObserveHistogramSampled_maskZeroAlwaysObserves(t *testing.T) {
	spy := &spyObserver{}
	var seq atomic.Uint64
	start := monotonicNano()

	for i := 0; i < 10; i++ {
		observeHistogramSampled(&seq, 0, spy, start)
	}
	assert.Equal(t, 10, spy.count())
}

// Tracks sampled histogram observe cost at configured sample rate.
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

// Tracks always-on histogram observe cost when mask is zero.
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

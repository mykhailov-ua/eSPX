package ingestion

import (
	"sync/atomic"
	_ "unsafe"

	"github.com/prometheus/client_golang/prometheus"
)

//go:linkname monotonicNano runtime.nanotime
func monotonicNano() int64

// nanosPerSecond converts monotonic nanoseconds to Prometheus seconds.
const nanosPerSecond = 1_000_000_000

// monoElapsedSeconds measures elapsed time immune to wall-clock jumps.
func monoElapsedSeconds(start int64) float64 {
	return float64(monotonicNano()-start) / nanosPerSecond
}

// luaMetricsSampleMask default downsampling rate to keep Redis Lua histogram overhead negligible.
const luaMetricsSampleMask uint64 = 127

// histogramSampleMaskFromConfig maps config to a sample mask; zero means observe every sample.
func histogramSampleMaskFromConfig(cfgVal int) uint64 {
	if cfgVal < 0 {
		return 0
	}
	if cfgVal == 0 {
		return luaMetricsSampleMask
	}
	return uint64(cfgVal)
}

// shouldSampleHistogram decides whether this sequence number should emit a histogram observation.
func shouldSampleHistogram(seq uint64, mask uint64) bool {
	if mask == 0 {
		return true
	}
	return seq&mask == 0
}

// shouldSampleLuaMetrics applies the default Lua latency sampling policy.
func shouldSampleLuaMetrics(seq uint64) bool {
	return shouldSampleHistogram(seq, luaMetricsSampleMask)
}

// observeHistogramSampled records elapsed monotonic time when the sample slot hits.
func observeHistogramSampled(seq *atomic.Uint64, mask uint64, observer prometheus.Observer, startMono int64) {
	if observer == nil {
		return
	}
	if shouldSampleHistogram(seq.Add(1), mask) {
		observer.Observe(monoElapsedSeconds(startMono))
	}
}

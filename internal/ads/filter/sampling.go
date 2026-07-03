package filter

import (
	"sync/atomic"
	_ "unsafe"

	"github.com/prometheus/client_golang/prometheus"
)

//go:linkname monotonicNano runtime.nanotime
func monotonicNano() int64

// MonotonicNano returns the current monotonic time in nanoseconds.
func MonotonicNano() int64 {
	return monotonicNano()
}

// NanosPerSecond converts monotonic nanoseconds to Prometheus seconds.
const NanosPerSecond = 1_000_000_000

func monoElapsedSeconds(start int64) float64 {
	return float64(monotonicNano()-start) / NanosPerSecond
}

// LuaMetricsSampleMask default downsampling rate to keep Redis Lua histogram overhead negligible.
const LuaMetricsSampleMask uint64 = 127

// luaMetricsSampleMask is the legacy name used within this package.
const luaMetricsSampleMask = LuaMetricsSampleMask

// HistogramSampleMaskFromConfig maps config to a sample mask; zero means observe every sample.
func HistogramSampleMaskFromConfig(cfgVal int) uint64 {
	if cfgVal < 0 {
		return 0
	}
	if cfgVal == 0 {
		return LuaMetricsSampleMask
	}
	return uint64(cfgVal)
}

func histogramSampleMaskFromConfig(cfgVal int) uint64 {
	return HistogramSampleMaskFromConfig(cfgVal)
}

// ShouldSampleHistogram decides whether this sequence number should emit a histogram observation.
func ShouldSampleHistogram(seq uint64, mask uint64) bool {
	if mask == 0 {
		return true
	}
	return seq&mask == 0
}

func shouldSampleHistogram(seq uint64, mask uint64) bool {
	return ShouldSampleHistogram(seq, mask)
}

func shouldSampleLuaMetrics(seq uint64) bool {
	return ShouldSampleHistogram(seq, LuaMetricsSampleMask)
}

// ObserveHistogramSampled records a histogram observation when seq matches mask.
func ObserveHistogramSampled(seq *atomic.Uint64, mask uint64, observer prometheus.Observer, startMono int64) {
	observeHistogramSampled(seq, mask, observer, startMono)
}

// ShouldSampleLuaMetrics reports whether seq should emit a Lua histogram sample.
func ShouldSampleLuaMetrics(seq uint64) bool {
	return shouldSampleLuaMetrics(seq)
}

func observeHistogramSampled(seq *atomic.Uint64, mask uint64, observer prometheus.Observer, startMono int64) {
	if observer == nil {
		return
	}
	if shouldSampleHistogram(seq.Add(1), mask) {
		observer.Observe(monoElapsedSeconds(startMono))
	}
}

// MonoElapsedSeconds measures elapsed monotonic time for ingest metrics.
func MonoElapsedSeconds(start int64) float64 {
	return monoElapsedSeconds(start)
}

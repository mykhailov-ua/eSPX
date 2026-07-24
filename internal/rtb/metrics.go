package rtb

import (
	"sync/atomic"
	_ "unsafe"

	"espx/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

// monotonicNano reads the runtime monotonic clock for sampled auction latency.
//
//go:linkname monotonicNano runtime.nanotime
func monotonicNano() int64

const (
	nanosPerSecond                     = 1_000_000_000
	rtbAuctionMetricsSampleMask uint64 = 127
)

type preboundAuctionMetrics struct {
	winTotal            prometheus.Counter
	spendRejected       prometheus.Counter
	duration            prometheus.Observer
	candidatesScanned   prometheus.Observer
	noBidInvalidRequest prometheus.Counter
	noBidEmptyShard     prometheus.Counter
	noBidCorruptCatalog prometheus.Counter
	noBidNoCandidates   prometheus.Counter
	noBidSpendFailed    prometheus.Counter
	noBidPacingClosed   prometheus.Counter
	noBidDailyCap       prometheus.Counter
	noBidScanLimit      prometheus.Counter
	noBidPrebidIVT      prometheus.Counter
	noBidSchainInvalid  prometheus.Counter
	noBidBreakerOpen    prometheus.Counter
}

var (
	auctionMetrics       preboundAuctionMetrics
	auctionMetricsInit   atomic.Bool
	metricsEnabled       atomic.Bool
	rtbAuctionMetricsSeq atomic.Uint64
)

func init() {
	metricsEnabled.Store(false)
	bindAuctionMetrics()
}

func bindAuctionMetrics() {
	if auctionMetricsInit.Swap(true) {
		return
	}
	auctionMetrics = preboundAuctionMetrics{
		winTotal:          metrics.RtbAuctionWinTotal,
		spendRejected:     metrics.RtbBudgetSpendRejectedTotal,
		duration:          metrics.RtbAuctionDuration,
		candidatesScanned: metrics.RtbAuctionCandidatesScanned,
		noBidInvalidRequest: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidInvalidRequest.String(),
		),
		noBidEmptyShard: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidEmptyShard.String(),
		),
		noBidCorruptCatalog: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidCorruptCatalog.String(),
		),
		noBidNoCandidates: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidNoCandidates.String(),
		),
		noBidSpendFailed: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidSpendFailed.String(),
		),
		noBidPacingClosed: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidPacingClosed.String(),
		),
		noBidDailyCap: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidDailyCapExceeded.String(),
		),
		noBidScanLimit: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidScanLimit.String(),
		),
		noBidPrebidIVT: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidPrebidIVT.String(),
		),
		noBidSchainInvalid: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidSchainInvalid.String(),
		),
		noBidBreakerOpen: metrics.RtbAuctionNoBidTotal.WithLabelValues(
			NoBidBreakerOpen.String(),
		),
	}
}

// SetMetricsEnabled toggles Prometheus recording for benchmarks and isolated tests.
func SetMetricsEnabled(enabled bool) {
	metricsEnabled.Store(enabled)
}

func auctionStartMono() int64 {
	if !metricsEnabled.Load() {
		return 0
	}
	return monotonicNano()
}

func recordAuctionWin(scanned int) {
	auctionMetrics.winTotal.Inc()
	auctionMetrics.candidatesScanned.Observe(float64(scanned))
}

func recordAuctionNoBid(reason NoBidReason, scanned int) {
	switch reason {
	case NoBidInvalidRequest:
		auctionMetrics.noBidInvalidRequest.Inc()
	case NoBidEmptyShard:
		auctionMetrics.noBidEmptyShard.Inc()
	case NoBidCorruptCatalog:
		auctionMetrics.noBidCorruptCatalog.Inc()
	case NoBidNoCandidates:
		auctionMetrics.noBidNoCandidates.Inc()
	case NoBidSpendFailed:
		auctionMetrics.noBidSpendFailed.Inc()
		auctionMetrics.spendRejected.Inc()
	case NoBidPacingClosed:
		auctionMetrics.noBidPacingClosed.Inc()
	case NoBidDailyCapExceeded:
		auctionMetrics.noBidDailyCap.Inc()
	case NoBidScanLimit:
		auctionMetrics.noBidScanLimit.Inc()
		metrics.RtbAuctionScanLimitTotal.Inc()
	case NoBidPrebidIVT:
		auctionMetrics.noBidPrebidIVT.Inc()
	case NoBidSchainInvalid:
		auctionMetrics.noBidSchainInvalid.Inc()
	case NoBidBreakerOpen:
		auctionMetrics.noBidBreakerOpen.Inc()
	}
	if scanned > 0 {
		auctionMetrics.candidatesScanned.Observe(float64(scanned))
	}
}

func observeAuctionDurationMono(start int64) {
	if start == 0 {
		return
	}
	seq := rtbAuctionMetricsSeq.Add(1)
	if seq&rtbAuctionMetricsSampleMask != 0 {
		return
	}
	elapsed := float64(monotonicNano()-start) / nanosPerSecond
	auctionMetrics.duration.Observe(elapsed)
}

func recordAuctionOutcome(start int64, reason NoBidReason, scanned int) {
	if !metricsEnabled.Load() {
		return
	}
	if reason.OK() {
		recordAuctionWin(scanned)
	} else {
		recordAuctionNoBid(reason, scanned)
	}
	observeAuctionDurationMono(start)
}

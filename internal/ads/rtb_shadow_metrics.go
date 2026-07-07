package ads

import (
	"sync/atomic"

	"espx/internal/domain"
	"espx/internal/metrics"
	"espx/internal/rtb"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

const rtbShadowPriceSampleMask uint64 = 127

type preboundRtbShadowMetrics struct {
	winnerMismatch prometheus.Counter
	noBid          map[rtb.NoBidReason]prometheus.Counter
	priceDelta     prometheus.Observer
}

var (
	rtbShadowMetrics     preboundRtbShadowMetrics
	rtbShadowMetricsInit atomic.Bool
	rtbShadowPriceSeq    atomic.Uint64
)

func bindRtbShadowMetrics() {
	if rtbShadowMetricsInit.Swap(true) {
		return
	}
	noBid := make(map[rtb.NoBidReason]prometheus.Counter, 8)
	for reason := rtb.NoBidInvalidRequest; reason <= rtb.NoBidDailyCapExceeded; reason++ {
		noBid[reason] = metrics.RtbShadowNoBidTotal.WithLabelValues(reason.String())
	}
	rtbShadowMetrics = preboundRtbShadowMetrics{
		winnerMismatch: metrics.RtbShadowWinnerMismatchTotal,
		noBid:          noBid,
		priceDelta:     metrics.RtbShadowPriceDeltaMicro,
	}
}

func init() {
	bindRtbShadowMetrics()
}

// recordRtbShadowAuction compares shadow eval output to the client campaign without heap allocation.
func recordRtbShadowAuction(
	catalog *RtbCatalog,
	evt *domain.Event,
	res rtb.AuctionResult,
	reason rtb.NoBidReason,
	payloadBidMicro int64,
) {
	if catalog == nil || evt == nil {
		return
	}
	if evt.CampaignID == uuid.Nil {
		return
	}
	if !reason.OK() {
		if counter, ok := rtbShadowMetrics.noBid[reason]; ok {
			counter.Inc()
		}
	} else {
		shadowWinner, ok := catalog.UUIDForWinner(res.CampaignID)
		if !ok || shadowWinner != evt.CampaignID {
			rtbShadowMetrics.winnerMismatch.Inc()
		}
	}
	recordRtbShadowDiff(catalog, evt, res, reason)
	if payloadBidMicro <= 0 {
		return
	}
	seq := rtbShadowPriceSeq.Add(1)
	if seq&rtbShadowPriceSampleMask != 0 {
		return
	}
	delta := res.Price - payloadBidMicro
	if delta < 0 {
		delta = -delta
	}
	rtbShadowMetrics.priceDelta.Observe(float64(delta))
}

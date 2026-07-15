package ingestion

import (
	"sync/atomic"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/rtb"
	"github.com/google/uuid"
)

const rtbShadowDiffBuckets = 24

// RtbShadowDiffSnapshotDTO aggregates shadow vs live-would-bid counters for an admin window.
type RtbShadowDiffSnapshotDTO struct {
	Window            string  `json:"window"`
	Source            string  `json:"source"`
	ShadowEvals       uint64  `json:"shadow_evals"`
	ShadowWinnerMatch uint64  `json:"shadow_winner_match"`
	ShadowMismatch    uint64  `json:"shadow_winner_mismatch"`
	ShadowNoBid       uint64  `json:"shadow_no_bid"`
	LiveWouldAccept   uint64  `json:"live_would_accept"`
	LiveWouldReject   uint64  `json:"live_would_reject"`
	ParityMatch       uint64  `json:"parity_match"`
	ParityRate        float64 `json:"parity_rate"`
	MismatchRate      float64 `json:"mismatch_rate"`
}

type rtbShadowDiffBucket struct {
	shadowEvals       atomic.Uint64
	shadowWinnerMatch atomic.Uint64
	shadowMismatch    atomic.Uint64
	shadowNoBid       atomic.Uint64
	liveWouldAccept   atomic.Uint64
	liveWouldReject   atomic.Uint64
	parityMatch       atomic.Uint64
}

var rtbShadowDiffRing [rtbShadowDiffBuckets]rtbShadowDiffBucket

func rtbShadowDiffBucketIdx(now time.Time) int {
	return now.UTC().Hour() % rtbShadowDiffBuckets
}

func recordRtbShadowDiff(catalog *RtbCatalog, evt *campaignmodel.Event, res rtb.AuctionResult, reason rtb.NoBidReason) {
	if catalog == nil || evt == nil || evt.CampaignID == uuid.Nil {
		return
	}
	b := &rtbShadowDiffRing[rtbShadowDiffBucketIdx(time.Now())]
	b.shadowEvals.Add(1)

	if !reason.OK() {
		b.shadowNoBid.Add(1)
		b.liveWouldReject.Add(1)
		b.parityMatch.Add(1)
		return
	}

	b.liveWouldAccept.Add(1)
	shadowWinner, ok := catalog.UUIDForWinner(res.CampaignID)
	if !ok || shadowWinner != evt.CampaignID {
		b.shadowMismatch.Add(1)
		return
	}
	b.shadowWinnerMatch.Add(1)
	b.parityMatch.Add(1)
}

// RtbShadowDiffForWindow aggregates in-memory hourly buckets for the requested duration.
func RtbShadowDiffForWindow(window time.Duration) RtbShadowDiffSnapshotDTO {
	if window <= 0 {
		window = time.Hour
	}
	hours := int(window.Hours())
	if hours < 1 {
		hours = 1
	}
	if hours > rtbShadowDiffBuckets {
		hours = rtbShadowDiffBuckets
	}

	now := time.Now().UTC()
	var snap RtbShadowDiffSnapshotDTO
	snap.Window = window.String()
	snap.Source = "memory"

	for i := 0; i < hours; i++ {
		idx := (now.Hour() - i + rtbShadowDiffBuckets) % rtbShadowDiffBuckets
		b := &rtbShadowDiffRing[idx]
		snap.ShadowEvals += b.shadowEvals.Load()
		snap.ShadowWinnerMatch += b.shadowWinnerMatch.Load()
		snap.ShadowMismatch += b.shadowMismatch.Load()
		snap.ShadowNoBid += b.shadowNoBid.Load()
		snap.LiveWouldAccept += b.liveWouldAccept.Load()
		snap.LiveWouldReject += b.liveWouldReject.Load()
		snap.ParityMatch += b.parityMatch.Load()
	}

	if snap.ShadowEvals > 0 {
		snap.ParityRate = float64(snap.ParityMatch) / float64(snap.ShadowEvals)
		snap.MismatchRate = float64(snap.ShadowMismatch) / float64(snap.ShadowEvals)
	}
	return snap
}

// ResetRtbShadowDiffBuckets clears in-memory counters (tests only).
func ResetRtbShadowDiffBuckets() {
	for i := range rtbShadowDiffRing {
		b := &rtbShadowDiffRing[i]
		b.shadowEvals.Store(0)
		b.shadowWinnerMatch.Store(0)
		b.shadowMismatch.Store(0)
		b.shadowNoBid.Store(0)
		b.liveWouldAccept.Store(0)
		b.liveWouldReject.Store(0)
		b.parityMatch.Store(0)
	}
}

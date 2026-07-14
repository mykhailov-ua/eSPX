package ads

import (
	"sync/atomic"
	"time"
)

const udpCoarseTimeClampMs = 50

// clockTickPausedUntil is monotonic-ns until background wall-clock tick may advance cachedUnixMilli.
var clockTickPausedUntil atomic.Int64

const udpCoarseClampNs = int64(udpCoarseTimeClampMs) * int64(time.Millisecond)

// applyUDPCoarseTime aligns cached wall millis from a control datagram (±50 ms clamp; never step backward).
func applyUDPCoarseTime(coarseTimeNs int64) {
	if coarseTimeNs <= 0 {
		return
	}
	remoteMs := coarseTimeNs / int64(time.Millisecond)
	localMs := cachedUnixMilli.Load()
	deltaMs := remoteMs - localMs
	if deltaMs > udpCoarseTimeClampMs {
		deltaMs = udpCoarseTimeClampMs
	} else if deltaMs < -udpCoarseTimeClampMs {
		deltaMs = -udpCoarseTimeClampMs
	}
	targetMs := localMs + deltaMs
	if targetMs < localMs {
		// Local wall is ahead of UDP coarse time: freeze tick until remote catches up.
		behindMs := localMs - targetMs
		clockTickPausedUntil.Store(monotonicNano() + behindMs*int64(time.Millisecond))
		return
	}
	if targetMs > localMs {
		cachedUnixMilli.Store(targetMs)
		cachedUnixMilliAny.Store(targetMs)
		t := time.UnixMilli(targetMs).UTC()
		cachedNowUTC.Store(&t)
		clockTickPausedUntil.Store(0)
	}
}

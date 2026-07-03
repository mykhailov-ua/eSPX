package log

import (
	"sync/atomic"
	"time"
)

var testSyncDelayNanos atomic.Int64

// SetSyncDelayForTest injects artificial latency before partition fsync (tests and lab chaos only).
func SetSyncDelayForTest(d time.Duration) {
	testSyncDelayNanos.Store(int64(d))
}

func testSyncDelay() time.Duration {
	return time.Duration(testSyncDelayNanos.Load())
}

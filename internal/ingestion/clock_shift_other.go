//go:build !linux

package ingestion

import (
	"fmt"
	"runtime"
	"time"
)

func shiftSystemClock(d time.Duration) (restore func(), err error) {
	return nil, fmt.Errorf("shiftSystemClock unsupported on %s", runtime.GOOS)
}

func refreshCachedWallClockNow() {
	ms := time.Now().UnixMilli()
	cachedUnixMilli.Store(ms)
	cachedUnixMilliAny.Store(ms)
	storeCachedNowUTC()
}

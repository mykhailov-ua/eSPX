//go:build !linux

package clock

import (
	"fmt"
	"runtime"
	"time"
)

func shiftSystemClock(d time.Duration) (restore func(), err error) {
	return nil, fmt.Errorf("shiftSystemClock unsupported on %s", runtime.GOOS)
}

func refreshCachedWallClockNow() {
	RefreshWallClockNow()
}

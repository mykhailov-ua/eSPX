//go:build linux

package ingestion

import (
	"fmt"
	"syscall"
	"time"
)

// shiftSystemClock adjusts the process-visible system clock by d and returns a restore func.
// Requires CAP_SYS_TIME; callers should fall back to shiftCachedWallClock when this fails.
func shiftSystemClock(d time.Duration) (restore func(), err error) {
	var tv syscall.Timeval
	if err := syscall.Gettimeofday(&tv); err != nil {
		return nil, fmt.Errorf("gettimeofday: %w", err)
	}
	orig := syscall.Timeval{Sec: tv.Sec, Usec: tv.Usec}

	deltaSec := d / time.Second
	deltaUsec := int64((d % time.Second) / time.Microsecond)
	tv.Sec += int64(deltaSec)
	tv.Usec += deltaUsec
	if tv.Usec >= 1_000_000 {
		tv.Sec += tv.Usec / 1_000_000
		tv.Usec %= 1_000_000
	}

	if err := syscall.Settimeofday(&tv); err != nil {
		return nil, fmt.Errorf("settimeofday: %w", err)
	}
	refreshCachedWallClockNow()

	return func() {
		_ = syscall.Settimeofday(&orig)
		refreshCachedWallClockNow()
	}, nil
}

func refreshCachedWallClockNow() {
	ms := time.Now().UnixMilli()
	cachedUnixMilli.Store(ms)
	cachedUnixMilliAny.Store(ms)
	storeCachedNowUTC()
}

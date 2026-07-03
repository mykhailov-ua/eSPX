package clock

import "time"

// ShiftSystemClock adjusts OS wall clock when privileged; used by chaos integration tests.
func ShiftSystemClock(d time.Duration) (restore func(), err error) {
	return shiftSystemClock(d)
}

// ShiftCachedWallClock advances cached wall-clock millis without syscall for TTC chaos tests.
func ShiftCachedWallClock(d time.Duration) (restore func(), err error) {
	SetClockRefreshPaused(true)
	before := cachedUnixMilli.Load()
	shifted := before + d.Milliseconds()
	cachedUnixMilli.Store(shifted)
	CachedUnixMilliAny.Store(shifted)
	t := time.UnixMilli(shifted).UTC()
	cachedNowUTC.Store(&t)

	return func() {
		cachedUnixMilli.Store(before)
		CachedUnixMilliAny.Store(before)
		storeCachedNowUTC()
		SetClockRefreshPaused(false)
	}, nil
}

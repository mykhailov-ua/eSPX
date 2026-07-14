package ads

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApplyUDPCoarseTime_clampAndNoBackward(t *testing.T) {
	SetClockRefreshPaused(true)
	defer SetClockRefreshPaused(false)

	before := int64(1_700_000_000_000)
	cachedUnixMilli.Store(before)
	cachedUnixMilliAny.Store(before)

	applyUDPCoarseTime((before + 200) * int64(time.Millisecond))
	require.Equal(t, before+50, cachedUnixMilli.Load())

	local := cachedUnixMilli.Load()
	applyUDPCoarseTime((local - 200) * int64(time.Millisecond))
	require.Equal(t, local, cachedUnixMilli.Load())
	require.Greater(t, clockTickPausedUntil.Load(), int64(0))
}

func TestApplyUDPCoarseTime_forwardWithinClamp(t *testing.T) {
	SetClockRefreshPaused(true)
	defer SetClockRefreshPaused(false)

	base := int64(1_700_000_000_000)
	cachedUnixMilli.Store(base)
	cachedUnixMilliAny.Store(base)
	clockTickPausedUntil.Store(0)

	applyUDPCoarseTime((base + 30) * int64(time.Millisecond))
	require.Equal(t, base+30, cachedUnixMilli.Load())
}

package ingest

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"espx/internal/ads/clock"
	"espx/internal/ads/filter"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	clockDriftSeconds       = 3600
	clockDriftWallSleep     = 5 * time.Second
	clockDriftFilterTimeout = 100 * time.Millisecond
	clockDriftTTCMin        = 3 * time.Second
)

// TestChaos_ClockDriftMonotonicTTC automates CHAOS.md §6 scenario D: +3600s wall-clock drift
// must not expire monotonic filter deadlines or falsely reject a click 5s after impression.
func TestChaos_ClockDriftMonotonicTTC(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	stack := startAdsIngestStackWithFilterTimeout(t, infra, "ads-chaos-clock-drift", int(clockDriftFilterTimeout.Milliseconds()))
	defer stack.Close(t)

	ctx := context.Background()
	userID := "chaos-clock-user"
	streamLenBefore, err := infra.Redis.XLen(ctx, stack.Stream).Result()
	require.NoError(t, err)

	require.Equal(t, http.StatusAccepted, postChaosImpression(t, stack.Handler, stack.CampaignID, userID))

	impTSKey := "fcap:c:" + stack.CampaignID.String() + ":u:" + userID + ":ts"
	impTS, err := infra.Redis.Get(ctx, impTSKey).Int64()
	require.NoError(t, err)

	time.Sleep(clockDriftWallSleep)

	restore, driftMethod, err := applyChaosClockDrift(time.Duration(clockDriftSeconds) * time.Second)
	require.NoError(t, err)
	defer restore()

	deadlineCtx := filter.AttachFilterDeadline(context.Background(), clockDriftFilterTimeout)
	time.Sleep(50 * time.Millisecond)
	require.False(t, filter.FilterDeadlineExceeded(deadlineCtx),
		"monotonic filter deadline must not expire after wall-clock drift")

	eventsBefore := countChaosCampaignEvents(t, infra.Pool, stack.CampaignID)
	status := postChaosTrack(t, stack.Handler, stack.CampaignID, "click", userID, uuid.NewString())
	require.Equal(t, http.StatusAccepted, status)

	streamLenAfter, err := infra.Redis.XLen(ctx, stack.Stream).Result()
	require.NoError(t, err)
	require.Equal(t, streamLenBefore+2, streamLenAfter,
		"impression and click must reach Redis stream (low_ttc aborts before XADD)")

	nowMs := clock.CachedUnixMilli()
	elapsedMs := nowMs - impTS
	require.GreaterOrEqual(t, elapsedMs, clockDriftTTCMin.Milliseconds(),
		"TTC window must exceed configured minimum after drift")

	require.Eventually(t, func() bool {
		return countChaosCampaignEvents(t, infra.Pool, stack.CampaignID) > eventsBefore
	}, 10*time.Second, 100*time.Millisecond, "click must settle to Postgres after drift")

	logChaosProof(t, "clock_drift_monotonic_safety", map[string]string{
		"drift_seconds": strconv.Itoa(clockDriftSeconds),
		"ttc_passed":    "true",
		"drift_method":  driftMethod,
		"filter_ms":     strconv.Itoa(100),
	})
}

func applyChaosClockDrift(d time.Duration) (restore func(), method string, err error) {
	if restore, err := clock.ShiftSystemClock(d); err == nil {
		return restore, "system_clock", nil
	}
	restore, err = clock.ShiftCachedWallClock(d)
	if err != nil {
		return nil, "", err
	}
	return restore, "cached_wall_clock", nil
}

// TestClockDrift_filterDeadlineSurvivesWallShift guards filter timeout uses monotonic time only.
func TestClockDrift_filterDeadlineSurvivesWallShift(t *testing.T) {
	ctx := filter.AttachFilterDeadline(context.Background(), clockDriftFilterTimeout)
	restore, err := clock.ShiftCachedWallClock(time.Duration(clockDriftSeconds) * time.Second)
	require.NoError(t, err)
	defer restore()

	time.Sleep(30 * time.Millisecond)
	assert.False(t, filter.FilterDeadlineExceeded(ctx))

	evt := &domain.Event{}
	engine := filter.NewFilterEngine(clockDriftFilterTimeout, &countingFilter{})
	err = engine.Check(ctx, evt)
	assert.NoError(t, err)
}

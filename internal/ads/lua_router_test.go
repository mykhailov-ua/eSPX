package ads

import (
	"testing"
	"time"

	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestUnifiedFilter_needsFullLuaPath(t *testing.T) {
	f := NewUnifiedFilter(nil, nil, &mockRegistry{}, nil, 0, time.Minute, time.Hour, time.Hour, 100, 10, "events", 1000)
	camp := &domain.Campaign{PacingMode: domain.PacingModeAsap}
	evt := &domain.Event{Type: "impression", CampaignID: uuid.New(), UserID: "u1"}

	f.SetLuaFastPathEnabled(true)
	require.False(t, f.needsFullLuaPath(evt, camp))

	f.SetTTCMin(time.Second)
	require.True(t, f.needsFullLuaPath(evt, camp))

	f.SetTTCMin(0)
	f.SetLuaFastPathEnabled(false)
	require.True(t, f.needsFullLuaPath(evt, camp))

	f.SetLuaFastPathEnabled(true)
	f.rateLimit = 10
	require.True(t, f.needsFullLuaPath(evt, camp))
	f.rateLimit = 0

	camp.FreqLimit = 3
	require.True(t, f.needsFullLuaPath(evt, camp))
	camp.FreqLimit = 0

	camp.PacingMode = domain.PacingModeEven
	require.True(t, f.needsFullLuaPath(evt, camp))
}

func TestUnifiedFilter_fastPathDebitMatchesFull(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := attachFilterDeadline(t.Context(), time.Second)
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	fFast := newRealRedisUnifiedFilter(t, rdb)
	fFast.SetLuaFastPathEnabled(true)
	fFast.SetTTCMin(0)
	require.NoError(t, fFast.PreloadScripts(ctx))

	fFull := newRealRedisUnifiedFilter(t, rdb)
	fFull.SetLuaFastPathEnabled(false)
	require.NoError(t, fFull.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	evtFast := &domain.Event{
		Type:       "impression",
		IP:         "203.0.113.90",
		UserID:     "fast",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	require.NoError(t, fFast.Check(ctx, evtFast))

	evtFull := &domain.Event{
		Type:       "impression",
		IP:         "203.0.113.91",
		UserID:     "full",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	require.NoError(t, fFull.Check(ctx, evtFull))

	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	remaining, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	const debitMicro = 10_000
	const expected = int64(9_000_000_000_000_000) - 2*debitMicro
	require.Equal(t, expected, remaining)
}

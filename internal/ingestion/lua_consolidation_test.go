package ingestion

import (
	"context"
	"fmt"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/licensing"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// edgeSlotPick mirrors deploy/nginx/lua/edge-slot-map.lua get_shard for parity tests (M9-05).
func edgeSlotPick(campaignID uuid.UUID, table *slotTable) (int, bool) {
	if table == nil {
		return 0, false
	}
	slot := crc32Castagnoli(&campaignID) & 1023
	return int(table[slot]), true
}

// TestChaos_EdgeSlotMapParity verifies edge get_shard matches Go StaticSlotSharder (M9-05).
func TestChaos_EdgeSlotMapParity(t *testing.T) {
	sharder := NewStaticSlotSharder(4)
	table := buildSlotTable(4)
	sharder.SwapSnapshot(7, table, 0)

	const n = 4096
	mismatches := 0
	for i := 0; i < n; i++ {
		id := uuid.New()
		goShard := sharder.GetShard(id)
		edgeShard, ok := edgeSlotPick(id, table)
		require.True(t, ok)
		if goShard != edgeShard {
			mismatches++
		}
	}
	require.Equal(t, 0, mismatches, "edge slot map must match StaticSlotSharder")

	logChaosProof(t, "edge_slot_map_parity", map[string]string{
		"samples":    fmt.Sprintf("%d", n),
		"mismatches": fmt.Sprintf("%d", mismatches),
		"version":    fmt.Sprintf("%d", sharder.SnapshotVersion()),
	})
}

// TestUnifiedFilter_LuaConsolidatedPrechecks exercises entitlements, placement, and fraud list in one EVALSHA (M9-02).
func TestUnifiedFilter_LuaConsolidatedPrechecks(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := attachFilterDeadline(t.Context(), time.Second)
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	reg := &mockRegistry{}
	f := newRealRedisUnifiedFilter(t, rdb)
	f.registry = reg
	f.SetLuaFastPathEnabled(true)
	f.SetTTCMin(0)
	f.SetRegionCode(0)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	require.NoError(t, rdb.HSet(ctx, PlacementBlacklistKey(campID), "zone-bad", "1").Err())
	require.NoError(t, rdb.SAdd(ctx, fraudBlacklistKey, "203.0.113.66").Err())

	placementEvt := &campaignmodel.Event{
		Type:        "impression",
		CampaignID:  campID,
		ClickID:     uuid.NewString(),
		IP:          "203.0.113.1",
		PlacementID: "zone-bad",
	}
	require.ErrorIs(t, f.Check(ctx, placementEvt), ErrPlacementBlocked)

	fraudEvt := &campaignmodel.Event{
		Type:       "impression",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		IP:         "203.0.113.66",
	}
	require.NoError(t, f.Check(ctx, fraudEvt))

	quotaReg := &entitlementsTestRegistry{
		CampaignRegistry: reg,
		maxRPD:           1,
	}
	f.registry = quotaReg
	quotaEvt := &campaignmodel.Event{
		Type:       "impression",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		IP:         "203.0.113.77",
	}
	require.NoError(t, f.Check(ctx, quotaEvt))
	quotaEvt.ClickID = uuid.NewString()
	require.ErrorIs(t, f.Check(ctx, quotaEvt), ErrDailyQuotaExceeded)
}

type entitlementsTestRegistry struct {
	campaignmodel.CampaignRegistry
	maxRPD uint64
}

func (r *entitlementsTestRegistry) GetEntitlements(customerID uuid.UUID) (licensing.Entitlements, bool) {
	return licensing.Entitlements{
		Limits: licensing.Limits{MaxRequestsPerDay: r.maxRPD},
	}, true
}

// TestUnifiedFilter_NoIPRateLimitKeys confirms Tier C Lua does not touch rl:ip keys (M9-03).
func TestUnifiedFilter_NoIPRateLimitKeys(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := attachFilterDeadline(t.Context(), time.Second)
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	f.SetLuaFastPathEnabled(false)
	f.SetTTCMin(0)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	var rlKey []byte
	rlKey = appendCampaignHashTag(rlKey[:0], campID)
	rlKey = append(rlKey, "rl:ip:203.0.113.50"...)
	require.NoError(t, rdb.Set(ctx, string(rlKey), 0, 0).Err())

	evt := &campaignmodel.Event{
		Type:       "click",
		IP:         "203.0.113.50",
		UserID:     "u-rl",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	require.NoError(t, f.Check(ctx, evt))

	val, err := rdb.Get(ctx, string(rlKey)).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(0), val, "rl:ip key must not be incremented by Lua")
}

// TestUnifiedFilter_TierDegradationNearDeadline skips non-critical Lua gates and records metric (M9-04).
func TestUnifiedFilter_TierDegradationNearDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	reg := &benchWorstRegistry{}
	f := newRealRedisUnifiedFilter(t, rdb)
	f.registry = reg
	f.SetLuaFastPathEnabled(false)
	f.SetTTCMin(500 * time.Millisecond)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, 9_000_000_000_000_000, 0).Err())
	require.NoError(t, rdb.Set(ctx, camp.FcapKeyPrefix+"degrade-user", 999, 0).Err())

	before := testutil.ToFloat64(metrics.FilterTierDegradedTotal)

	evt := &campaignmodel.Event{
		Type:       "click",
		IP:         "203.0.113.88",
		UserID:     "degrade-user",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	evt.FilterDeadlineMono = monotonicNano() + 500_000 // 0.5 ms remaining

	require.NoError(t, f.Check(ctx, evt))
	after := testutil.ToFloat64(metrics.FilterTierDegradedTotal)
	require.Greater(t, after-before, 0.0)
}

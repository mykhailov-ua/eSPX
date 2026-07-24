package ingestion

import (
	"context"
	"strconv"
	"testing"
	"time"

	"espx/internal/campaignmodel"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// benchWorstRegistry returns a campaign that forces unified-filter.lua (rate, fcap, pacing, TTC).
type benchWorstRegistry struct {
	customerID uuid.UUID
	camp       *campaignmodel.Campaign
}

func (r *benchWorstRegistry) Exists(uuid.UUID) bool { return true }
func (r *benchWorstRegistry) Add(uuid.UUID, uuid.UUID, *uuid.UUID, string, campaignmodel.PacingMode, int64, string, int32, int32, []string) {
}
func (r *benchWorstRegistry) GetCustomerID(uuid.UUID) (uuid.UUID, bool) {
	if r.customerID == uuid.Nil {
		r.customerID = uuid.New()
	}
	return r.customerID, true
}
func (r *benchWorstRegistry) GetCampaign(id uuid.UUID) (*campaignmodel.Campaign, bool) {
	if r.camp != nil && r.camp.ID == id {
		return r.camp, true
	}
	custID, _ := r.GetCustomerID(id)
	cp := &campaignmodel.Campaign{
		ID:               id,
		CustomerID:       custID,
		PacingMode:       campaignmodel.PacingModeEven,
		DailyBudgetMicro: 1_000_000_000_000,
		FreqLimit:        100,
		FreqWindow:       3600,
		Location:         time.UTC,
	}
	enrichMockCampaign(cp)
	r.camp = cp
	return r.camp, true
}
func (*benchWorstRegistry) Sync(context.Context) (int, error) { return 0, nil }
func (*benchWorstRegistry) StartSync(context.Context, time.Duration) {
}
func (*benchWorstRegistry) Wait(context.Context) error { return nil }

func newLuaBenchFilter(b testing.TB, rdb redis.UniversalClient, reg campaignmodel.CampaignRegistry, rateLimit int) *UnifiedFilter {
	b.Helper()
	f := NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		NewJumpHashSharder(1),
		reg,
		nil,
		rateLimit,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10_000,
	)
	f.SetLuaFastPathEnabled(true)
	f.SetFilterEvalPinWorkers(1)
	if err := f.PreloadScripts(context.Background()); err != nil {
		b.Fatal(err)
	}
	return f
}

// BenchmarkLuaScript_Happy measures budget-fast.lua (Tier B) on impression fast path.
func BenchmarkLuaScript_Happy(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(b)
	defer cleanup()

	f := newLuaBenchFilter(b, rdb, &mockRegistry{}, 0)
	f.SetTTCMin(0)
	campID := uuid.New()
	seedCampaignBudget(b, ctx, rdb, campID)

	payload := []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"impression"}`)
	evt := &campaignmodel.Event{
		Type:       "impression",
		IP:         "203.0.113.210",
		UserID:     "bench-happy",
		CampaignID: campID,
		Payload:    payload,
	}
	setFilterDeadlineOnEvent(evt, time.Second)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		evt.ClickID = unsafeString(strconv.AppendInt(evt.ClickIDBuf[:0], int64(i), 10))
		if err := f.Check(ctx, evt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLuaScript_Worst measures unified-filter.lua with rate, fcap, even pacing, and TTC.
func BenchmarkLuaScript_Worst(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(b)
	defer cleanup()

	reg := &benchWorstRegistry{}
	f := newLuaBenchFilter(b, rdb, reg, 100_000)
	f.SetTTCMin(500 * time.Millisecond)
	campID := uuid.New()
	seedCampaignBudget(b, ctx, rdb, campID)

	camp, ok := reg.GetCampaign(campID)
	if !ok {
		b.Fatal("campaign setup failed")
	}
	nowMs := time.Now().UnixMilli()
	requireNoError := func(err error) {
		if err != nil {
			b.Fatal(err)
		}
	}
	requireNoError(rdb.Set(ctx, camp.FcapKeyPrefix+"bench-worst", 0, 0).Err())
	requireNoError(rdb.Set(ctx, camp.DailySpendKeyPrefix+time.Now().In(camp.Location).Format("20060102"), 0, 0).Err())
	var impKey []byte
	impKey = appendCampaignHashTag(impKey[:0], campID)
	impKey = append(impKey, "imp_ts:"...)
	impKey = append(impKey, "bench-worst"...)
	impKey = append(impKey, ':')
	impKey = appendUUID(impKey, campID)
	requireNoError(rdb.Set(ctx, string(impKey), strconv.FormatInt(nowMs, 10), time.Hour).Err())

	payload := []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click"}`)
	evt := &campaignmodel.Event{
		Type:       "click",
		IP:         "203.0.113.211",
		UserID:     "bench-worst",
		CampaignID: campID,
		Payload:    payload,
	}
	setFilterDeadlineOnEvent(evt, 2*time.Second)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		evt.ClickID = unsafeString(strconv.AppendInt(evt.ClickIDBuf[:0], int64(i), 10))
		if err := f.Check(ctx, evt); err != nil {
			b.Fatal(err)
		}
	}
}

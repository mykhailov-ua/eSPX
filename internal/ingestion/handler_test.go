package ingestion

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/ingestion/pb"
	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"google.golang.org/protobuf/proto"
)

// In-memory campaign registry stub for handler and filter tests.
type mockRegistry struct{}

func (m *mockRegistry) Exists(id uuid.UUID) bool { return true }
func (m *mockRegistry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode campaignmodel.PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
}
func (m *mockRegistry) GetCustomerID(id uuid.UUID) (uuid.UUID, bool) { return uuid.Nil, true }

var (
	staticCampaignMu sync.RWMutex
	staticCampaign   = &campaignmodel.Campaign{CustomerID: uuid.Nil, Location: time.UTC}
	cachedMockCamp   atomic.Pointer[campaignmodel.Campaign]
)

func enrichMockCampaign(cp *campaignmodel.Campaign) {
	if cp.Location == nil {
		cp.Location = time.UTC
	}
	if cp.IDStr == "" {
		cp.IDStr = cp.ID.String()
	}
	if cp.IDStrAny == nil {
		cp.IDStrAny = cp.IDStr
	}
	if cp.CustomerIDStr == "" {
		cp.CustomerIDStr = cp.CustomerID.String()
	}
	if cp.CustomerIDStrAny == nil {
		cp.CustomerIDStrAny = cp.CustomerIDStr
	}
	if cp.BudgetCampaignKey == "" {
		cp.BudgetCampaignKey = "budget:campaign:" + cp.IDStr
	}
	if cp.CampaignSyncKey == "" {
		cp.CampaignSyncKey = "budget:sync:campaign:" + cp.IDStr
	}
	if cp.CustomerSyncKey == "" {
		cp.CustomerSyncKey = "budget:sync:customer:" + cp.CustomerIDStr
	}
	if cp.FcapKeyPrefix == "" {
		if cp.BrandFcapKey != "" {
			cp.FcapKeyPrefix = cp.BrandFcapKey + ":u:"
		} else {
			cp.FcapKeyPrefix = "fcap:c:" + cp.IDStr + ":u:"
		}
	}
	if cp.DailySpendKeyPrefix == "" {
		cp.DailySpendKeyPrefix = "budget:daily_spent:campaign:" + cp.IDStr + ":"
	}
	if cp.DailyBudgetMicroAny == nil && cp.DailyBudgetMicro != 0 {
		cp.DailyBudgetMicroAny = cp.DailyBudgetMicro
	}
}

func (m *mockRegistry) GetCampaign(id uuid.UUID) (*campaignmodel.Campaign, bool) {
	if got := cachedMockCamp.Load(); got != nil && got.ID == id {
		if got.BudgetCampaignKey == "" {
			cp := *got
			enrichMockCampaign(&cp)
			cachedMockCamp.Store(&cp)
		}
		return cachedMockCamp.Load(), true
	}

	staticCampaignMu.RLock()
	defer staticCampaignMu.RUnlock()

	cp := *staticCampaign
	cp.ID = id
	enrichMockCampaign(&cp)

	cachedMockCamp.Store(&cp)
	return cachedMockCamp.Load(), true
}
func (m *mockRegistry) Sync(ctx context.Context) (int, error)                 { return 0, nil }
func (m *mockRegistry) StartSync(ctx context.Context, interval time.Duration) {}
func (m *mockRegistry) Wait(ctx context.Context) error                        { return nil }

var staticRemoteAddr = &net.TCPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 1234}

// gnet.Conn stub capturing writes for packet handler benchmarks.
type mockGnetConn struct {
	gnet.Conn
	written []byte
	ctx     any
}

func (m *mockGnetConn) Context() any     { return m.ctx }
func (m *mockGnetConn) SetContext(v any) { m.ctx = v }

func (m *mockGnetConn) Write(b []byte) (int, error) {
	m.written = append(m.written[:0], b...)
	return len(b), nil
}

func (m *mockGnetConn) RemoteAddr() net.Addr {
	return staticRemoteAddr
}

// Tracks JSON gnet packet handler cost as legacy hot path baseline.
func BenchmarkAdsPacketHandlerJSON(b *testing.B) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewAdsPacketHandler(cfg, registry, nil, nil, nil, sharder, "fraud-stream", nil)

	payload := []byte(`{"campaign_id":"` + uuid.NewString() + `","user_id":"user123","type":"click","click_id":"click123","payload":{}}`)
	req := parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/json"),
		ClientIP:         []byte("1.1.1.1"),
		UserAgent:        []byte("Mozilla/5.0"),
		Body:             payload,
		ContentLength:    len(payload),
		HasContentLength: true,
	}

	conn := &mockGnetConn{written: make([]byte, 0, 512)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.React(req, conn)
	}
}

// Tracks protobuf gnet packet handler cost for production hot path.
func BenchmarkAdsPacketHandlerProto(b *testing.B) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewAdsPacketHandler(cfg, registry, nil, nil, nil, sharder, "fraud-stream", nil)

	pbPayload := &pb.AdEvent{
		CampaignId: []byte(uuid.NewString()),
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("test-click"),
		},
	}
	body, _ := proto.Marshal(pbPayload)
	req := parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/x-protobuf"),
		ClientIP:         []byte("1.1.1.1"),
		UserAgent:        []byte("Mozilla/5.0"),
		Body:             body,
		ContentLength:    len(body),
		HasContentLength: true,
	}

	conn := &mockGnetConn{written: make([]byte, 0, 512)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.React(req, conn)
	}
}

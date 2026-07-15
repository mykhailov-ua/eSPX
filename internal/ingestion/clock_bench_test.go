package ingestion

import (
	"context"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/ingestion/pb"
	"github.com/google/uuid"
)

func buildProtoTrackPayload(b *testing.B) []byte {
	b.Helper()
	id := uuid.New()
	pbPayload := &pb.AdEvent{
		CampaignId: id[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("test-click"),
		},
	}
	body, err := pbPayload.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}
	return body
}

// Tracks time.Now syscall cost as wall-clock baseline.
func BenchmarkHotPath_timeNow(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Now()
	}
}

// Tracks monotonic nano time cost as preferred hot path clock.
func BenchmarkHotPath_monotonicNano(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = monotonicNano()
	}
}

// Tracks cached UTC time cost against direct syscalls.
func BenchmarkHotPath_cachedTimeUTC(b *testing.B) {
	storeCachedNowUTC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CachedTimeUTC()
	}
}

// Tracks filter engine deadline check overhead per request.
func BenchmarkHotPath_filterEngineDeadlineCheck(b *testing.B) {
	ctx := attachFilterDeadline(context.Background(), 5*time.Second)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filterDeadlineExceeded(ctx)
	}
}

// Tracks filter engine check without timeout on hot path.
func BenchmarkHotPath_filterEngineCheck_noTimeout(b *testing.B) {
	engine := NewFilterEngine(0, &countingFilter{})
	evt := &campaignmodel.Event{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = engine.Check(ctx, evt)
	}
}

// Tracks filter engine check with deadline on hot path.
func BenchmarkHotPath_filterEngineCheck_withDeadline(b *testing.B) {
	engine := NewFilterEngine(5*time.Second, &countingFilter{})
	evt := &campaignmodel.Event{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = engine.Check(ctx, evt)
	}
}

// Tracks latency ring record overhead per request.
func BenchmarkHotPath_latencyRingRecord(b *testing.B) {
	ring := NewLatencyRing(defaultLatencyRingCap)
	start := monotonicNano()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.RecordMono(start)
	}
}

// Tracks prebound counter increment cost per request.
func BenchmarkHotPath_counterInc(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filterGeoLookupErrors.Inc()
	}
}

// Tracks full protobuf accept path including filters and metrics.
func BenchmarkHotPath_AdsPacketHandlerProto_accept(b *testing.B) {
	BenchmarkAdsPacketHandlerProto(b)
}

// Tracks protobuf reject path cost for unknown campaign.
func BenchmarkHotPath_AdsPacketHandlerProto_reject404(b *testing.B) {
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewAdsPacketHandler(cfg, registry, NewFilterEngine(0, &errFilter{err: ErrCampaignNotFound}), nil, nil, sharder, "fraud-stream", nil)

	payload := buildProtoTrackPayload(b)
	req := parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/x-protobuf"),
		Body:             payload,
		ContentLength:    len(payload),
		HasContentLength: true,
	}
	conn := &mockGnetConn{written: make([]byte, 0, 512)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.React(req, conn)
	}
}

// Tracks protobuf infra error path cost for capacity planning.
func BenchmarkHotPath_AdsPacketHandlerProto_infra503(b *testing.B) {
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	handler := NewAdsPacketHandler(cfg, &mockRegistry{}, NewFilterEngine(0, infraErrFilter{}), nil, nil, NewJumpHashSharder(1), "fraud-stream", nil)

	payload := buildProtoTrackPayload(b)
	req := parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/x-protobuf"),
		Body:             payload,
		ContentLength:    len(payload),
		HasContentLength: true,
	}
	conn := &mockGnetConn{written: make([]byte, 0, 512)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.React(req, conn)
	}
}

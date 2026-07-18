package ingestion

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"github.com/google/uuid"
)

func benchWritePathEvent() *campaignmodel.Event {
	return &campaignmodel.Event{
		ClickID:    "bench-click",
		CampaignID: uuid.New(),
		Type:       "click",
		IP:         "203.0.113.1",
		UA:         "bench-agent",
		Payload:    []byte(`{"bench":true}`),
		CreatedAt:  time.Unix(1_700_000_000, 0).UTC(),
	}
}

// BenchmarkCHSpoolAppendDurably measures mmap WAL append + fsync on the cold write path.
func BenchmarkCHSpoolAppendDurably(b *testing.B) {
	dir := b.TempDir()
	spool, err := OpenCHSpool(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = spool.Close() }()

	evt := benchWritePathEvent()
	events := []*campaignmodel.Event{evt}
	token := "bench-dedup-token"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := spool.AppendDurably(token, events); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCHSpoolMarshalPayload isolates vtproto encode cost without fsync.
func BenchmarkCHSpoolMarshalPayload(b *testing.B) {
	evt := benchWritePathEvent()
	events := []*campaignmodel.Event{evt}
	token := "bench-dedup-token"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := marshalCHSpoolPayload(token, events); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPostgresStoreBatch_Mock measures in-memory mock StoreBatch (processor store leg).
func BenchmarkPostgresStoreBatch_Mock(b *testing.B) {
	store := &MockEventStore{}
	evt := benchWritePathEvent()
	events := []*campaignmodel.Event{evt}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := store.StoreBatch(ctx, events); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClickHouseStoreBatch_Spooled measures CH outage path: spool append without live CH.
func BenchmarkClickHouseStoreBatch_Spooled(b *testing.B) {
	dir := b.TempDir()
	spool, err := OpenCHSpool(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = spool.Close() }()

	conn := newFailingCHConn(true)
	store := NewClickHouseStore(conn, time.Second, "", DefaultCHSpoolConfig(), nil)
	store.SetSpool(spool)

	evt := benchWritePathEvent()
	events := []*campaignmodel.Event{evt}
	ctx := context.WithValue(context.Background(), campaignmodel.DeduplicationTokenKey, "bench-ch-spool")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := store.StoreBatch(ctx, events); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCHSpoolOpenFdDelta reports FD count before/after spool open (linux only).
func BenchmarkCHSpoolOpenFdDelta(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("requires /proc/self/fd")
	}
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("fd_before=%d", len(before))

	dir := b.TempDir()
	spool, err := OpenCHSpool(dir)
	if err != nil {
		b.Fatal(err)
	}
	afterOpen, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("fd_after_open=%d delta=%d", len(afterOpen), len(afterOpen)-len(before))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = spool.WritePos()
	}
	_ = spool.Close()
}

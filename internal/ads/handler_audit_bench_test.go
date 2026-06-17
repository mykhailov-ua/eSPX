package ads

import (
	"sync/atomic"
	"testing"
	"time"

	"espx/pkg/logger"

	"github.com/google/uuid"
)

// Tracks sampled impression audit log cost on accept path.
func BenchmarkHandler_auditLog_impression_sampled(b *testing.B) {
	cfg := logger.Config{
		LogDir:           b.TempDir(),
		FlushBufferSize:  256 * 1024,
		RotateSize:       512 * 1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := logger.NewLogger(cfg, 1)
	defer l.Close()

	var seq atomic.Uint64
	campID := uuid.New()
	ts := time.Now().Unix()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeAuditLog(l, &seq, 127, 0, ts, campID, "bench-click", "impression")
	}
}

// Tracks always-on click audit log cost on accept path.
func BenchmarkHandler_auditLog_click_always(b *testing.B) {
	cfg := logger.Config{
		LogDir:           b.TempDir(),
		FlushBufferSize:  256 * 1024,
		RotateSize:       512 * 1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := logger.NewLogger(cfg, 1)
	defer l.Close()

	var seq atomic.Uint64
	campID := uuid.New()
	ts := time.Now().Unix()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeAuditLog(l, &seq, 127, 0, ts, campID, "bench-click", "click")
	}
}

// Tracks unsampled impression audit log cost at full volume.
func BenchmarkHandler_auditLog_impression_unsampled(b *testing.B) {
	cfg := logger.Config{
		LogDir:           b.TempDir(),
		FlushBufferSize:  256 * 1024,
		RotateSize:       512 * 1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := logger.NewLogger(cfg, 1)
	defer l.Close()

	var seq atomic.Uint64
	campID := uuid.New()
	ts := time.Now().Unix()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeAuditLog(l, &seq, 0, 0, ts, campID, "bench-click", "impression")
	}
}

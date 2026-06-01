package logger

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoggerZeroAlloc(t *testing.T) {
	cfg := Config{
		LogDir:           t.TempDir(),
		FlushBufferSize:  4096,
		RotateSize:       1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()
	data := []byte("high-performance zero-allocation production telemetry test log line")
	l.WriteToShard(0, 1, data)
	allocs := testing.AllocsPerRun(1000, func() {
		ok := l.WriteToShard(0, 1, data)
		if !ok {
			t.Fatal("write failed")
		}
	})
	if allocs > 0 {
		t.Errorf("Hot-path write produced %f allocations, expected 0", allocs)
	}
}

func TestLoggerRingBufferOverflow(t *testing.T) {
	s := NewLogShard()
	data := []byte("overflow testing line")
	for i := 0; i < RingCapacity; i++ {
		ok := s.Write(0, data)
		if !ok {
			t.Fatalf("early drop at %d", i)
		}
	}
	ok := s.Write(0, data)
	if ok {
		t.Fatal("expected drop on saturation")
	}
}

func TestLoggerDiskDegradationEmergency(t *testing.T) {
	cfg := Config{
		LogDir:           t.TempDir(),
		FlushBufferSize:  4096,
		RotateSize:       1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()
	l.diskDegraded.Store(1)
	data := []byte("telemetry log entry")
	ok := l.WriteToShard(0, 0, data)
	if !ok {
		t.Fatal("write failed")
	}
	buf := l.getBuffer()
	buf, _ = l.drainShards(buf)
	dropped := l.loadSheddingEvents.Load()
	if dropped != 1 {
		t.Errorf("expected 1 dropped log, got %d", dropped)
	}
	if buf.offset > 0 {
		t.Error("expected empty buffer")
	}
}

func TestLoggerRotation(t *testing.T) {
	logDir := t.TempDir()
	cfg := Config{
		LogDir:           logDir,
		FlushBufferSize:  4096,
		RotateSize:       10,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()
	data := []byte("some data to force rotation")
	l.WriteToShard(0, 1, data)
	time.Sleep(100 * time.Millisecond)
	l.WriteToShard(0, 1, data)
	time.Sleep(100 * time.Millisecond)
	pattern := filepath.Join(logDir, "segment_*.log.ready")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("expected rotated segment file ending with .ready")
	}
}

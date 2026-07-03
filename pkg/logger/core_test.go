package logger

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSendBufferEnqueueTimeout verifies load shedding when the persist queue is saturated.
func TestSendBufferEnqueueTimeout(t *testing.T) {
	l := &Logger{
		cfg: Config{
			FlushBufferSize:       4096,
			PersistEnqueueTimeout: 2 * time.Millisecond,
		},
		persistCh:       make(chan *AlignedBuffer, 1),
		persistQueueCap: 1,
		closeChan:       make(chan struct{}),
	}
	l.persistCh <- NewAlignedBuffer(4096)

	buf := NewAlignedBuffer(4096)
	buf.offset = 512
	l.sendBuffer(buf, false)

	if l.persistQueueDrops.Load() != 1 {
		t.Fatalf("drops=%d want 1", l.persistQueueDrops.Load())
	}
	if l.persistQueueDropBytes.Load() != 512 {
		t.Fatalf("drop bytes=%d want 512", l.persistQueueDropBytes.Load())
	}
}

// TestLoggerZeroAlloc enforces zero-allocation on the hot WriteToShard path.
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

// TestLogShardMPSCConcurrent stress-tests the ring under parallel producers.
func TestLogShardMPSCConcurrent(t *testing.T) {
	const (
		producers = 8
		perProd   = 2000
	)
	s := NewLogShard()
	line := []byte("mpsc concurrent log line payload")
	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perProd; i++ {
				if !s.Write(1, line) {
					t.Error("write failed under load")
					return
				}
			}
		}()
	}
	wg.Wait()

	want := uint64(producers * perProd)
	if got := atomic.LoadUint64(&s.writeCursor); got != want {
		t.Fatalf("writeCursor=%d want %d", got, want)
	}
	if got := atomic.LoadUint64(&s.allocCursor); got != want {
		t.Fatalf("allocCursor=%d want %d", got, want)
	}

	buf := NewAlignedBuffer(4 * 1024 * 1024)
	l := &Logger{
		cfg:    Config{FlushBufferSize: 4 * 1024 * 1024},
		shards: []*LogShard{s},
	}
	buf, _ = l.drainShards(buf)
	if buf.offset == 0 {
		t.Fatal("drain produced empty buffer")
	}
}

// TestLogShardMPSCUniqueLines ensures distinct lines survive MPSC load without slot reuse loss.
func TestLogShardMPSCUniqueLines(t *testing.T) {
	const producers = 16
	s := NewLogShard()
	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		p := p
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				msg := []byte("p=" + strconv.Itoa(p) + " i=" + strconv.Itoa(i))
				if !s.Write(1, msg) {
					t.Error("write failed")
					return
				}
			}
		}()
	}
	wg.Wait()

	buf := NewAlignedBuffer(8 * 1024 * 1024)
	l := &Logger{
		cfg:    Config{FlushBufferSize: 8 * 1024 * 1024},
		shards: []*LogShard{s},
	}
	buf, _ = l.drainShards(buf)
	data := buf.Bytes()
	lines := make(map[string]bool)
	for len(data) > 0 {
		if len(data) < 4 {
			t.Fatal("malformed length prefix")
		}
		length := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < length {
			t.Fatal("truncated payload")
		}
		lines[string(data[:length])] = true
		data = data[length:]
	}
	for p := 0; p < producers; p++ {
		for i := 0; i < 500; i++ {
			needle := "p=" + strconv.Itoa(p) + " i=" + strconv.Itoa(i)
			if !lines[needle] {
				t.Fatalf("missing line %q in drained output", needle)
			}
		}
	}
}

// TestLoggerRingBufferOverflow confirms full rings return false instead of blocking producers.
func TestLoggerRingBufferOverflow(t *testing.T) {
	s := NewLogShard()
	data := []byte("overflow testing line")
	for i := 0; i < ringUsable; i++ {
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

// TestLoggerDiskDegradationEmergency verifies priority-0 logs are shed when disk is degraded.
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

// TestLoggerDiskDegradation_keepsCriticalPriority keeps priority-1 lines during degradation.
func TestLoggerDiskDegradation_keepsCriticalPriority(t *testing.T) {
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

	low := []byte("impression audit line")
	high := []byte("click audit line")
	if !l.WriteToShard(0, 0, low) {
		t.Fatal("low priority write failed")
	}
	if !l.WriteToShard(0, 1, high) {
		t.Fatal("high priority write failed")
	}

	buf := l.getBuffer()
	buf, _ = l.drainShards(buf)
	if l.loadSheddingEvents.Load() != 1 {
		t.Fatalf("shedding=%d want 1 low-priority drop", l.loadSheddingEvents.Load())
	}
	if buf.offset == 0 {
		t.Fatal("expected critical log in drain buffer")
	}
}

// TestLoggerRotation checks size-based roll produces evac-ready compressed segments.
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

	var matches []string
	for i := 0; i < 40; i++ {
		matches, _ = filepath.Glob(filepath.Join(logDir, "segment_*.log.zst.ready"))
		if len(matches) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if len(matches) == 0 {
		t.Fatal("expected rotated segment file ending with .ready")
	}
}

// TestLoggerEncryptionDecryption validates the on-disk segment format for evac tooling.
func TestLoggerEncryptionDecryption(t *testing.T) {
	t.Setenv("LOG_ENCRYPTION_KEY", "test-super-secret-passphrase")

	logDir := t.TempDir()
	cfg := Config{
		LogDir:                logDir,
		FlushBufferSize:       4096,
		RotateSize:            10,
		RotateInterval:        time.Hour,
		DiskLatencyLimit:      time.Second,
		PersistEnqueueTimeout: time.Second,
	}

	l := NewLogger(cfg, 1)

	lines := [][]byte{
		[]byte("first high-performance encrypted log line"),
		[]byte("second high-performance encrypted log line"),
		[]byte("third high-performance encrypted log line"),
	}

	for _, line := range lines {
		ok := l.WriteToShard(0, 1, line)
		if !ok {
			t.Fatal("write to shard failed")
		}
		time.Sleep(100 * time.Millisecond)
	}

	var readyMatches []string
	for i := 0; i < 40; i++ {
		readyMatches, _ = filepath.Glob(filepath.Join(logDir, "segment_*.log.zst.ready"))
		if len(readyMatches) >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	l.Close()

	key := DeriveKey("test-super-secret-passphrase")

	var decryptedBytes []byte
	for _, path := range readyMatches {
		dec, err := DecryptSegment(path, key)
		if err != nil {
			t.Fatalf("failed to decrypt segment %s: %v", path, err)
		}
		decryptedBytes = append(decryptedBytes, dec...)
	}

	activePath := filepath.Join(logDir, "active.log")
	if activeData, err := os.ReadFile(activePath); err == nil && len(activeData) > 0 {
		decryptedBytes = append(decryptedBytes, activeData...)
	}

	data := decryptedBytes
	for _, expectedLine := range lines {
		if len(data) < 4 {
			t.Fatalf("decrypted data too short, expected length prefix")
		}
		length := binary.BigEndian.Uint32(data[:4])
		data = data[4:]
		if uint32(len(data)) < length {
			t.Fatalf("decrypted data too short, expected payload of length %d", length)
		}
		actualLine := data[:length]
		data = data[length:]

		if string(actualLine) != string(expectedLine) {
			t.Errorf("mismatch: got %q, want %q", actualLine, expectedLine)
		}
	}
}

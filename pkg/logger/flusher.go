package logger

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

type AlignedBuffer struct {
	raw     []byte
	aligned []byte
	offset  int
}

func NewAlignedBuffer(size int) *AlignedBuffer {
	raw := make([]byte, size+4096)
	ptr := uintptr(unsafe.Pointer(&raw[0]))
	misalignment := ptr & 4095
	var offset uintptr
	if misalignment != 0 {
		offset = 4096 - misalignment
	}
	aligned := raw[offset : offset+uintptr(size)]
	return &AlignedBuffer{
		raw:     raw,
		aligned: aligned,
		offset:  0,
	}
}

func (b *AlignedBuffer) Write(data []byte) int {
	n := copy(b.aligned[b.offset:], data)
	b.offset += n
	return n
}

func (b *AlignedBuffer) WriteByte(c byte) {
	b.aligned[b.offset] = c
	b.offset++
}

func (b *AlignedBuffer) Reset() {
	b.offset = 0
}

func (b *AlignedBuffer) Bytes() []byte {
	return b.aligned[:b.offset]
}

func (b *AlignedBuffer) Available() int {
	return len(b.aligned) - b.offset
}

var bufferPool sync.Pool

func (l *Logger) getBuffer() *AlignedBuffer {
	val := bufferPool.Get()
	if val == nil {
		return NewAlignedBuffer(l.cfg.FlushBufferSize)
	}
	buf := val.(*AlignedBuffer)
	if len(buf.aligned) < l.cfg.FlushBufferSize {
		return NewAlignedBuffer(l.cfg.FlushBufferSize)
	}
	return buf
}

func (l *Logger) StartDrainer() {
	defer l.wg.Done()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	buf := l.getBuffer()
	var firstLogAt time.Time
	for {
		select {
		case <-l.closeChan:
			buf, _ = l.drainShards(buf)
			l.sendBuffer(buf)
			close(l.persistCh)
			return
		case <-ticker.C:
			var flushed bool
			buf, flushed = l.drainShards(buf)
			if flushed && firstLogAt.IsZero() {
				firstLogAt = time.Now()
			}
			if buf.offset > 0 {
				if buf.offset >= 64*1024 || time.Since(firstLogAt) >= 50*time.Millisecond {
					l.sendBuffer(buf)
					buf = l.getBuffer()
					firstLogAt = time.Time{}
				}
			}
		}
	}
}

func (l *Logger) drainShards(buf *AlignedBuffer) (*AlignedBuffer, bool) {
	degraded := l.diskDegraded.Load() == 1
	var flushed bool
	for _, shard := range l.shards {
		writeCursor := atomic.LoadUint64(&shard.writeCursor)
		readCursor := atomic.LoadUint64(&shard.readCursor)
		for readCursor < writeCursor {
			idx := readCursor & RingMask
			payload := &shard.slots[idx]
			if degraded && payload.Priority == 0 {
				l.loadSheddingEvents.Add(1)
				readCursor++
				continue
			}
			logBytes := payload.Data[:payload.Length]
			totalSize := len(logBytes) + 1
			if buf.Available() < totalSize {
				l.sendBuffer(buf)
				buf = l.getBuffer()
			}
			buf.Write(logBytes)
			buf.WriteByte('\n')
			flushed = true
			readCursor++
		}
		atomic.StoreUint64(&shard.readCursor, readCursor)
	}
	return buf, flushed
}

func (l *Logger) sendBuffer(buf *AlignedBuffer) {
	if buf.offset == 0 {
		bufferPool.Put(buf)
		return
	}
	select {
	case l.persistCh <- buf:
	default:
		l.loadSheddingEvents.Add(uint64(buf.offset / 100))
		buf.Reset()
		bufferPool.Put(buf)
	}
}

func (l *Logger) StartPersister() {
	defer l.wg.Done()
	lastStatCheck := time.Now()
	for buf := range l.persistCh {
		l.writeBuffer(buf)
		buf.Reset()
		bufferPool.Put(buf)
		if time.Since(lastStatCheck) >= 5*time.Second {
			l.checkDiskSpace()
			lastStatCheck = time.Now()
		}
	}
}

func (l *Logger) writeBuffer(buf *AlignedBuffer) {
	l.checkRotation()
	if l.diskDegraded.Load() == 1 {
		l.loadSheddingEvents.Add(uint64(buf.offset / 100))
		return
	}
	data := buf.Bytes()
	start := time.Now()
	n, err := l.activeFile.Write(data)
	duration := time.Since(start)
	LogNVMEWriteDurationSeconds.Observe(duration.Seconds())
	if err != nil {
		l.diskDegraded.Store(1)
		l.loadSheddingEvents.Add(uint64(buf.offset / 100))
		return
	}
	latencyMs := float64(duration.Nanoseconds()) / 1e6
	currentEMA := math.Float64frombits(l.emaLatency.Load())
	var newEMA float64
	if currentEMA == 0 {
		newEMA = latencyMs
	} else {
		newEMA = 0.1*latencyMs + 0.9*currentEMA
	}
	l.emaLatency.Store(math.Float64bits(newEMA))
	if newEMA > float64(l.cfg.DiskLatencyLimit.Milliseconds()) {
		l.diskDegraded.Store(1)
	}
	l.bytesWritten += int64(n)
}

func (l *Logger) checkDiskSpace() {
	var stat syscall.Statfs_t
	err := syscall.Statfs(l.cfg.LogDir, &stat)
	if err != nil {
		l.diskDegraded.Store(1)
		return
	}
	freeSpace := stat.Bavail * uint64(stat.Bsize)
	if freeSpace < 1024*1024*1024 {
		l.diskDegraded.Store(1)
	} else {
		ema := math.Float64frombits(l.emaLatency.Load())
		if ema <= float64(l.cfg.DiskLatencyLimit.Milliseconds()) {
			l.diskDegraded.Store(0)
		}
	}
}

func (l *Logger) checkRotation() {
	if l.activeFile == nil {
		l.openActiveFile()
		return
	}
	sizeReached := l.bytesWritten >= l.cfg.RotateSize
	timeReached := time.Since(l.fileOpenedAt) >= l.cfg.RotateInterval
	if sizeReached || timeReached {
		_ = l.activeFile.Close()
		timestamp := time.Now().Format("20060102-150405.000000000")
		rotatedPath := filepath.Join(l.cfg.LogDir, fmt.Sprintf("segment_%s.log.ready", timestamp))
		activePath := filepath.Join(l.cfg.LogDir, "active.log")
		_ = os.Rename(activePath, rotatedPath)
		LogRotationTotal.Inc()
		l.openActiveFile()
	}
}

func (l *Logger) openActiveFile() {
	_ = os.MkdirAll(l.cfg.LogDir, 0755)
	activePath := filepath.Join(l.cfg.LogDir, "active.log")
	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		l.diskDegraded.Store(1)
		return
	}
	l.activeFile = f
	l.fileOpenedAt = time.Now()
	l.bytesWritten = 0
}

package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// StartPersister serializes disk appends through one goroutine so fsync contention stays off the drain and ingest paths.
func (l *Logger) StartPersister() {
	defer l.wg.Done()
	for buf := range l.persistCh {
		l.writeBuffer(buf)
		buf.Reset()
		bufferPool.Put(buf)
	}
}

// writeBuffer appends a batch with fdatasync and updates latency EMA so the logger can enter degraded mode before NVMe backs up the hot path.
func (l *Logger) writeBuffer(buf *AlignedBuffer) {
	l.checkRotation()
	if l.diskDegraded.Load() == 1 {
		l.loadSheddingEvents.Add(uint64(buf.offset / 100))
		return
	}
	data := buf.Bytes()
	start := time.Now()

	n, err := l.activeFile.Write(data)
	if err == nil {
		err = syscall.Fdatasync(int(l.activeFile.Fd()))
	}
	duration := time.Since(start)
	LogNVMEWriteDurationSeconds.Observe(duration.Seconds())
	if err != nil {
		l.diskDegraded.Store(1)
		l.loadSheddingEvents.Add(uint64(buf.offset / 100))
		return
	}
	latencyNs := uint64(duration.Nanoseconds())
	currentEMA := l.emaLatency.Load()
	var newEMA uint64
	if currentEMA == 0 {
		newEMA = latencyNs
	} else {
		newEMA = (latencyNs + 9*currentEMA) / 10
	}
	l.emaLatency.Store(newEMA)
	if newEMA > uint64(l.cfg.DiskLatencyLimit.Nanoseconds()) {
		l.diskDegraded.Store(1)
	}
	l.bytesWritten += int64(n)
}

// checkDiskSpace toggles degraded mode from free space and write latency so billing logs survive before the disk fills or NVMe stalls.
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
		ema := l.emaLatency.Load()
		if ema <= uint64(l.cfg.DiskLatencyLimit.Nanoseconds()) {
			l.diskDegraded.Store(0)
		} else {
			l.emaLatency.Store(0)
			l.diskDegraded.Store(0)
		}
	}
}

// StartDiskMonitor periodically re-evaluates disk health so degraded shedding lifts only when storage is safe again.
func (l *Logger) StartDiskMonitor() {
	defer l.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-l.closeChan:
			return
		case <-ticker.C:
			l.checkDiskSpace()
		}
	}
}

// checkRotation rolls active.log into bounded segments so log-evacuate can ship files without unbounded growth.
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
		rotatedPath := filepath.Join(l.cfg.LogDir, fmt.Sprintf("segment_%s.log", timestamp))
		activePath := filepath.Join(l.cfg.LogDir, "active.log")
		_ = os.Rename(activePath, rotatedPath)
		LogRotationTotal.Inc()
		l.openActiveFile()
	}
}

// openActiveFile creates or reopens the writable segment after startup, rotation, or a prior disk error.
func (l *Logger) openActiveFile() {
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

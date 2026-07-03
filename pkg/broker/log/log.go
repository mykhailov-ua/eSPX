// Package log implements mmap-backed append-only segments for broker partition storage.
package log

import (
	"encoding/binary"
	"errors"
	"espx/internal/metrics"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	ErrSegmentNotFound   = errors.New("segment not found")
	ErrStaleFencingEpoch = errors.New("stale fencing epoch")
	ErrReplicationGap    = errors.New("replication gap: unexpected offset")
)

const fencingEpochFile = "fencing.epoch"

// FetchBufPool reuses 1 MiB fetch buffers to keep broker read paths allocation-free.
var FetchBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 1024*1024)
		return &b
	},
}

// Segment is one mmap-backed log and sparse index pair for a contiguous offset range.
type Segment struct {
	baseOffset uint64
	logFile    *os.File
	indexFile  *os.File
	logPath    string
	indexPath  string
	logSize    int64
	indexSize  int64

	mmapData   []byte
	mmapIndex  []byte
	maxSegSize int64
	maxIdxSize int64
}

// findActualIndexSize trims a sparse index file to valid entries after crash or partial writes.
func findActualIndexSize(idxData []byte, baseOffset uint64) int64 {
	numEntries := len(idxData) / 16
	var count int64 = 0
	var lastOffset uint64 = 0
	hasLast := false

	for i := 0; i < numEntries; i++ {
		off := binary.BigEndian.Uint64(idxData[i*16 : i*16+8])
		pos := int64(binary.BigEndian.Uint64(idxData[i*16+8 : i*16+16]))

		if off == 0 && pos == 0 && i > 0 {
			break
		}
		if off < baseOffset {
			break
		}
		if pos < 0 {
			break
		}
		if hasLast && off <= lastOffset {
			break
		}

		lastOffset = off
		hasLast = true
		count++
	}

	return count * 16
}

// NewSegment opens or creates one log segment with mmap backing for append or read-only fetch.
func NewSegment(dir string, baseOffset uint64, maxSegSize int64, indexInterval int64, writeable bool) (*Segment, error) {
	logName := fmt.Sprintf("%020d.log", baseOffset)
	idxName := fmt.Sprintf("%020d.index", baseOffset)
	logPath := filepath.Join(dir, logName)
	indexPath := filepath.Join(dir, idxName)

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", logPath, err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("failed to open index file %s: %w", indexPath, err)
	}

	logInfo, err := logFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, err
	}

	logSize := logInfo.Size()
	idxInfo, err := indexFile.Stat()
	if err != nil {
		_ = logFile.Close()
		_ = indexFile.Close()
		return nil, err
	}
	indexSize := idxInfo.Size()

	if indexSize > 0 {
		idxData := make([]byte, indexSize)
		if _, err := io.ReadFull(indexFile, idxData); err == nil {
			indexSize = findActualIndexSize(idxData, baseOffset)
		}
		_, _ = indexFile.Seek(0, io.SeekStart)
	}

	if indexInterval <= 0 {
		indexInterval = 4096
	}
	maxIdxSize := (maxSegSize/indexInterval + 100) * 16

	var mmapData []byte
	var mmapIndex []byte

	if writeable {
		if logSize < maxSegSize {
			if err := logFile.Truncate(maxSegSize); err != nil {
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, err
			}
		}
		mmapData, err = syscall.Mmap(int(logFile.Fd()), 0, int(maxSegSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			_ = logFile.Close()
			_ = indexFile.Close()
			return nil, fmt.Errorf("log mmap failed: %w", err)
		}

		for i := 0; i < len(mmapData); i += 4096 {
			_ = mmapData[i]
		}

		if indexSize < maxIdxSize {
			if err := indexFile.Truncate(maxIdxSize); err != nil {
				_ = syscall.Munmap(mmapData)
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, err
			}
		}
		mmapIndex, err = syscall.Mmap(int(indexFile.Fd()), 0, int(maxIdxSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			_ = syscall.Munmap(mmapData)
			_ = logFile.Close()
			_ = indexFile.Close()
			return nil, fmt.Errorf("index mmap failed: %w", err)
		}

		for i := 0; i < len(mmapIndex); i += 4096 {
			_ = mmapIndex[i]
		}
	} else {
		if logSize > 0 {
			mmapData, err = syscall.Mmap(int(logFile.Fd()), 0, int(logSize), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, fmt.Errorf("log mmap failed: %w", err)
			}
			madvise(mmapData, syscall.MADV_WILLNEED)
		}
		if indexSize > 0 {
			mmapIndex, err = syscall.Mmap(int(indexFile.Fd()), 0, int(indexSize), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				if len(mmapData) > 0 {
					_ = syscall.Munmap(mmapData)
				}
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, fmt.Errorf("index mmap failed: %w", err)
			}
			madvise(mmapIndex, syscall.MADV_WILLNEED)
		}
	}

	return &Segment{
		baseOffset: baseOffset,
		logFile:    logFile,
		indexFile:  indexFile,
		logPath:    logPath,
		indexPath:  indexPath,
		logSize:    logSize,
		indexSize:  indexSize,
		mmapData:   mmapData,
		mmapIndex:  mmapIndex,
		maxSegSize: maxSegSize,
		maxIdxSize: maxIdxSize,
	}, nil
}

// madvise hints the kernel to prefetch mmap pages on cold segment opens.
func madvise(data []byte, advice int) {
	if len(data) == 0 {
		return
	}
	ptr := unsafe.Pointer(unsafe.SliceData(data))
	_, _, _ = syscall.Syscall(syscall.SYS_MADVISE, uintptr(ptr), uintptr(len(data)), uintptr(advice))
}

// Close unmmaps segment files so rolled segments can be truncated and reopened safely.
func (s *Segment) Close() error {
	var errs []error
	if len(s.mmapData) > 0 {
		if err := syscall.Munmap(s.mmapData); err != nil {
			errs = append(errs, err)
		}
		s.mmapData = nil
	}
	if len(s.mmapIndex) > 0 {
		if err := syscall.Munmap(s.mmapIndex); err != nil {
			errs = append(errs, err)
		}
		s.mmapIndex = nil
	}
	if err := s.logFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := s.indexFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Write appends one record directly into the mmap log without syscall per message.
func (s *Segment) Write(offset uint64, payload []byte) (int64, error) {
	payloadLen := len(payload)
	length := uint32(8 + payloadLen)
	totalLen := 12 + payloadLen
	pos := atomic.LoadInt64(&s.logSize)

	if pos+int64(totalLen) > s.maxSegSize {
		return 0, errors.New("segment space exhausted")
	}

	basePtr := unsafe.Pointer(unsafe.SliceData(s.mmapData))
	recordPtr := unsafe.Pointer(uintptr(basePtr) + uintptr(pos))

	*(*uint32)(recordPtr) = bits.ReverseBytes32(length)

	offsetPtr := unsafe.Pointer(uintptr(recordPtr) + 4)
	*(*uint64)(offsetPtr) = bits.ReverseBytes64(offset)

	if payloadLen > 0 {
		payloadDst := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(recordPtr)+12)), payloadLen)
		copy(payloadDst, payload)
	}

	atomic.StoreInt64(&s.logSize, pos+int64(totalLen))
	return pos, nil
}

// WriteIndexEntry records a sparse offset-to-position mapping so fetch avoids full log scans.
func (s *Segment) WriteIndexEntry(offset uint64, position int64) error {
	idxSize := atomic.LoadInt64(&s.indexSize)
	if idxSize+16 > s.maxIdxSize {
		return errors.New("index space exhausted")
	}

	basePtr := unsafe.Pointer(unsafe.SliceData(s.mmapIndex))
	entryPtr := unsafe.Pointer(uintptr(basePtr) + uintptr(idxSize))

	*(*uint64)(entryPtr) = bits.ReverseBytes64(offset)

	posPtr := unsafe.Pointer(uintptr(entryPtr) + 8)
	*(*uint64)(posPtr) = bits.ReverseBytes64(uint64(position))

	atomic.StoreInt64(&s.indexSize, idxSize+16)
	return nil
}

// FindPosition binary-searches the sparse index to locate the log byte offset for a message offset.
func (s *Segment) FindPosition(offset uint64) (int64, error) {
	idxSize := atomic.LoadInt64(&s.indexSize)
	n := idxSize / 16
	if n == 0 {
		return 0, nil
	}

	low := int64(0)
	high := n - 1
	var bestPos int64 = 0

	for low <= high {
		mid := (low + high) / 2
		off := binary.BigEndian.Uint64(s.mmapIndex[mid*16 : mid*16+8])
		if off <= offset {
			bestPos = int64(binary.BigEndian.Uint64(s.mmapIndex[mid*16+8 : mid*16+16]))
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return bestPos, nil
}

// Recover replays the log tail after crash and truncates torn records so offsets stay monotonic.
func (s *Segment) Recover() (uint64, error) {
	idxInfo, err := s.indexFile.Stat()
	if err != nil {
		return s.baseOffset, err
	}

	idxSize := idxInfo.Size()
	if idxSize > 0 {
		idxData := make([]byte, idxSize)
		if _, err := s.indexFile.ReadAt(idxData, 0); err == nil {
			idxSize = findActualIndexSize(idxData, s.baseOffset)
		}
	}

	var lastIdxOffset uint64 = s.baseOffset
	var lastIdxPos int64 = 0

	if idxSize >= 16 {
		if len(s.mmapIndex) >= int(idxSize) {
			lastIdxOffset = binary.BigEndian.Uint64(s.mmapIndex[idxSize-16 : idxSize-8])
			lastIdxPos = int64(binary.BigEndian.Uint64(s.mmapIndex[idxSize-8 : idxSize]))
		}
	}

	atomic.StoreInt64(&s.indexSize, idxSize)

	currentOffset := lastIdxOffset
	currentPos := lastIdxPos
	mmapSize := int64(len(s.mmapData))

	for {
		if currentPos+12 > mmapSize {
			break
		}

		length := binary.BigEndian.Uint32(s.mmapData[currentPos : currentPos+4])
		offset := binary.BigEndian.Uint64(s.mmapData[currentPos+4 : currentPos+12])

		if length == 0 && offset == 0 {
			break
		}

		payloadLen := int64(length) - 8
		if payloadLen < 0 || currentPos+12+payloadLen > mmapSize {
			break
		}

		currentOffset = offset + 1
		currentPos += 12 + payloadLen
	}

	atomic.StoreInt64(&s.logSize, currentPos)
	if err := s.logFile.Truncate(currentPos); err != nil {
		return currentOffset, err
	}

	if len(s.mmapData) > 0 {
		_ = syscall.Munmap(s.mmapData)
		s.mmapData = nil
	}
	if len(s.mmapIndex) > 0 {
		_ = syscall.Munmap(s.mmapIndex)
		s.mmapIndex = nil
	}

	if mmapSize == s.maxSegSize {
		if err := s.logFile.Truncate(s.maxSegSize); err != nil {
			return currentOffset, err
		}
		s.mmapData, err = syscall.Mmap(int(s.logFile.Fd()), 0, int(s.maxSegSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return currentOffset, err
		}
		if err := s.indexFile.Truncate(s.maxIdxSize); err != nil {
			return currentOffset, err
		}
		s.mmapIndex, err = syscall.Mmap(int(s.indexFile.Fd()), 0, int(s.maxIdxSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	} else {
		if currentPos > 0 {
			s.mmapData, err = syscall.Mmap(int(s.logFile.Fd()), 0, int(currentPos), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				return currentOffset, err
			}
		}
		if idxSize > 0 {
			s.mmapIndex, err = syscall.Mmap(int(s.indexFile.Fd()), 0, int(idxSize), syscall.PROT_READ, syscall.MAP_SHARED)
		}
	}
	return currentOffset, err
}

// LocateMessages scans from an index position and bounds the fetch window by maxBytes.
func (s *Segment) LocateMessages(indexPos int64, startOffset uint64, maxBytes uint32) (int64, uint32, uint32, error) {
	logSize := atomic.LoadInt64(&s.logSize)
	currentPos := indexPos
	var targetPos int64 = -1
	var msgCount uint32 = 0
	var totalMsgBytes uint32 = 0

	for {
		if currentPos+12 > logSize {
			break
		}

		length := binary.BigEndian.Uint32(s.mmapData[currentPos : currentPos+4])
		offset := binary.BigEndian.Uint64(s.mmapData[currentPos+4 : currentPos+12])
		payloadLen := int64(length) - 8
		if payloadLen < 0 {
			break
		}
		recordLen := 12 + payloadLen

		if currentPos+recordLen > logSize {
			break
		}

		if offset >= startOffset {
			if targetPos == -1 {
				targetPos = currentPos
			}
			if targetPos != -1 {
				if totalMsgBytes+uint32(recordLen) > maxBytes && msgCount > 0 {
					return targetPos, msgCount, totalMsgBytes, nil
				}
				msgCount++
				totalMsgBytes += uint32(recordLen)
			}
		}
		currentPos += recordLen

		if targetPos != -1 && totalMsgBytes >= maxBytes {
			break
		}
	}

	if targetPos == -1 {
		return 0, 0, 0, io.EOF
	}

	return targetPos, msgCount, totalMsgBytes, nil
}

// Sync persists log and index data so followers and crash recovery see committed records.
func (s *Segment) Sync() error {
	var errs []error
	if err := s.logFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	if err := s.indexFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// segmentSnapshot is an immutable view of all segments published via atomic pointer swap.
type segmentSnapshot struct {
	segments  []*Segment
	activeSeg *Segment
}

// PartitionLog is the append-only topic log with segment rolling and lock-free reads.
type PartitionLog struct {
	writeMu       sync.Mutex
	dir           string
	snap          atomic.Pointer[segmentSnapshot]
	nextOffset    uint64
	bytesSinceIdx int64
	indexInterval int64
	maxSegSize    int64
	flushTicker   *time.Ticker
	closeChan     chan struct{}
	wg            sync.WaitGroup
	fencingEpoch  atomic.Uint64
	durability    DurabilityConfig
	pendingFsync  atomic.Int64

	DiskOK atomic.Bool
}

// NewPartitionLog opens or creates on-disk segments with default async durability.
func NewPartitionLog(dir string, maxSegSize int64, indexInterval int64) (*PartitionLog, error) {
	return NewPartitionLogWithDurability(dir, maxSegSize, indexInterval, DefaultDurabilityConfig())
}

// NewPartitionLogWithDurability opens a partition log with an explicit fsync policy.
func NewPartitionLogWithDurability(dir string, maxSegSize int64, indexInterval int64, cfg DurabilityConfig) (*PartitionLog, error) {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.GroupCommitRecords <= 0 {
		cfg.GroupCommitRecords = 64
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	p := &PartitionLog{
		dir:           dir,
		maxSegSize:    maxSegSize,
		indexInterval: indexInterval,
		closeChan:     make(chan struct{}),
		durability:    cfg,
	}
	p.DiskOK.Store(true)

	if err := p.loadSegments(); err != nil {
		return nil, err
	}

	p.startFlushLoop()
	return p, nil
}

// Durability returns the active fsync policy for this partition.
func (p *PartitionLog) Durability() DurabilityConfig {
	return p.durability
}

// PendingFsync exposes records waiting for group-commit fsync (tests and metrics).
func (p *PartitionLog) PendingFsync() int64 {
	return p.pendingFsync.Load()
}

// loadSegments discovers existing segment files and recovers the active tail on startup.
func (p *PartitionLog) loadSegments() error {
	files, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if strings.HasSuffix(f.Name(), ".log") {
			baseStr := strings.TrimSuffix(f.Name(), ".log")
			val, err := strconv.ParseUint(baseStr, 10, 64)
			if err != nil {
				continue
			}
			baseOffsets = append(baseOffsets, val)
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	var segments []*Segment
	for i, offset := range baseOffsets {
		isLast := i == len(baseOffsets)-1
		seg, err := NewSegment(p.dir, offset, p.maxSegSize, p.indexInterval, isLast)
		if err != nil {
			return err
		}
		segments = append(segments, seg)
	}

	if len(segments) == 0 {
		seg, err := NewSegment(p.dir, 0, p.maxSegSize, p.indexInterval, true)
		if err != nil {
			return err
		}
		segments = append(segments, seg)
	}

	active := segments[len(segments)-1]

	next, err := active.Recover()
	if err != nil {
		return fmt.Errorf("failed to recover active segment: %w", err)
	}
	p.nextOffset = next

	p.snap.Store(&segmentSnapshot{
		segments:  segments,
		activeSeg: active,
	})

	if err := p.loadFencingEpoch(); err != nil {
		return err
	}

	return nil
}

// loadFencingEpoch restores the highest accepted leader epoch from disk after restart.
func (p *PartitionLog) loadFencingEpoch() error {
	path := filepath.Join(p.dir, fencingEpochFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(data) < 8 {
		return nil
	}
	p.fencingEpoch.Store(binary.BigEndian.Uint64(data[:8]))
	return nil
}

// persistFencingEpoch atomically stores the fencing floor so stale leaders cannot resume after crash.
func (p *PartitionLog) persistFencingEpoch(epoch uint64) error {
	path := filepath.Join(p.dir, fencingEpochFile)
	tmpPath := path + ".tmp"
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], epoch)
	if err := os.WriteFile(tmpPath, buf[:], 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// FencingEpoch returns the highest leader epoch this partition has accepted.
func (p *PartitionLog) FencingEpoch() uint64 {
	return p.fencingEpoch.Load()
}

// AdvanceFencingEpoch raises the stored epoch floor when a newer leader term is known cluster-wide.
func (p *PartitionLog) AdvanceFencingEpoch(epoch uint64) error {
	for {
		cur := p.fencingEpoch.Load()
		if epoch <= cur {
			return nil
		}
		if !p.fencingEpoch.CompareAndSwap(cur, epoch) {
			continue
		}
		return p.persistFencingEpoch(epoch)
	}
}

// startFlushLoop fsyncs the active segment in the background for async and group-commit modes.
func (p *PartitionLog) startFlushLoop() {
	interval := p.durability.FlushInterval
	p.flushTicker = time.NewTicker(interval)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.flushTicker.C:
				if p.durability.Mode == DurabilitySync {
					continue
				}
				if p.durability.Mode == DurabilityGroupCommit {
					if p.pendingFsync.Load() > 0 {
						p.Sync()
						p.pendingFsync.Store(0)
					}
				} else {
					p.Sync()
				}
			case <-p.closeChan:
				return
			}
		}
	}()
}

// SegmentCount returns the number of on-disk segments including the active tail.
func (p *PartitionLog) SegmentCount() int {
	s := p.snap.Load()
	if s == nil {
		return 0
	}
	return len(s.segments)
}

// NextOffset returns the next assignable message offset for replication catch-up.
func (p *PartitionLog) NextOffset() uint64 {
	p.writeMu.Lock()
	off := p.nextOffset
	p.writeMu.Unlock()
	return off
}

// Append assigns the next offset without fencing checks (tests and standalone broker).
func (p *PartitionLog) Append(payload []byte) (uint64, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	offset, err := p.appendPayloadLocked(p.nextOffset, payload, false, 0)
	if err != nil {
		return 0, err
	}
	return offset, p.applyDurabilityAfterLeaderAppend()
}

// AppendReplicatedAt applies one leader log entry on a follower when offset matches nextOffset.
func (p *PartitionLog) AppendReplicatedAt(expectedOffset uint64, payload []byte) (uint64, error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	if expectedOffset < p.nextOffset {
		return expectedOffset, nil
	}
	if expectedOffset > p.nextOffset {
		return 0, ErrReplicationGap
	}
	return p.appendPayloadLocked(expectedOffset, payload, false, 0)
}

// AppendFenced appends when epoch meets the stored floor; epoch 0 skips fencing (standalone broker).
func (p *PartitionLog) AppendFenced(epoch uint64, payload []byte) (uint64, error) {
	if epoch == 0 {
		p.writeMu.Lock()
		defer p.writeMu.Unlock()
		offset, err := p.appendPayloadLocked(p.nextOffset, payload, false, 0)
		if err != nil {
			return 0, err
		}
		return offset, p.applyDurabilityAfterLeaderAppend()
	}
	max := p.fencingEpoch.Load()
	if epoch < max {
		return 0, ErrStaleFencingEpoch
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	offset, err := p.appendPayloadLocked(p.nextOffset, payload, true, epoch)
	if err != nil {
		return 0, err
	}
	return offset, p.applyDurabilityAfterLeaderAppend()
}

func (p *PartitionLog) applyDurabilityAfterLeaderAppend() error {
	switch p.durability.Mode {
	case DurabilitySync:
		p.syncLocked()
	case DurabilityGroupCommit:
		n := p.pendingFsync.Add(1)
		if n >= p.durability.GroupCommitRecords {
			p.pendingFsync.Store(0)
			p.syncLocked()
		}
	}
	return nil
}

func (p *PartitionLog) syncLocked() {
	p.timedSyncActive()
}

func (p *PartitionLog) timedSyncActive() {
	s := p.snap.Load()
	if s == nil || s.activeSeg == nil {
		return
	}
	if d := testSyncDelay(); d > 0 {
		time.Sleep(d)
	}
	start := time.Now()
	_ = s.activeSeg.Sync()
	metrics.BrokerFsyncDuration.Observe(time.Since(start).Seconds())
}

func (p *PartitionLog) appendPayloadLocked(offset uint64, payload []byte, trackEpoch bool, epoch uint64) (uint64, error) {
	if trackEpoch && epoch > p.fencingEpoch.Load() {
		p.fencingEpoch.Store(epoch)
		if err := p.persistFencingEpoch(epoch); err != nil {
			return 0, err
		}
	}

	s := p.snap.Load()
	activeSeg := s.activeSeg
	totalLen := int64(12 + len(payload))

	activeLogSize := atomic.LoadInt64(&activeSeg.logSize)
	if activeLogSize+totalLen > p.maxSegSize {
		if err := p.rollLocked(s); err != nil {
			return 0, err
		}
		s = p.snap.Load()
		activeSeg = s.activeSeg
	}
	pos, err := activeSeg.Write(offset, payload)
	if err != nil {
		return 0, err
	}

	p.nextOffset = offset + 1
	p.bytesSinceIdx += int64(12 + len(payload))

	if p.bytesSinceIdx >= p.indexInterval {
		if err := activeSeg.WriteIndexEntry(offset, pos); err != nil {
			return 0, err
		}
		p.bytesSinceIdx = 0
	}

	return offset, nil
}

// rollLocked seals the full segment and swaps in a new writable one without blocking readers.
func (p *PartitionLog) rollLocked(old *segmentSnapshot) error {
	activeSeg := old.activeSeg

	if err := activeSeg.Sync(); err != nil {
		return err
	}

	activeLogSize := atomic.LoadInt64(&activeSeg.logSize)
	if err := activeSeg.logFile.Truncate(activeLogSize); err != nil {
		return err
	}

	activeIdxSize := atomic.LoadInt64(&activeSeg.indexSize)
	if err := activeSeg.indexFile.Truncate(activeIdxSize); err != nil {
		return err
	}

	readOnlySeg, err := NewSegment(p.dir, activeSeg.baseOffset, p.maxSegSize, p.indexInterval, false)
	if err != nil {
		return err
	}

	newSeg, err := NewSegment(p.dir, p.nextOffset, p.maxSegSize, p.indexInterval, true)
	if err != nil {
		_ = readOnlySeg.Close()
		return err
	}

	newSegments := make([]*Segment, len(old.segments))
	copy(newSegments, old.segments)
	newSegments[len(old.segments)-1] = readOnlySeg
	newSegments = append(newSegments, newSeg)

	p.snap.Store(&segmentSnapshot{
		segments:  newSegments,
		activeSeg: newSeg,
	})

	go func(seg *Segment) {
		time.Sleep(100 * time.Millisecond)
		_ = seg.Close()
	}(activeSeg)

	p.bytesSinceIdx = 0
	return nil
}

// Sync fsyncs the active segment on demand from the background flush loop.
func (p *PartitionLog) Sync() {
	p.timedSyncActive()
}

// Close stops the flush loop and closes all segment files on broker shutdown.
func (p *PartitionLog) Close() error {
	close(p.closeChan)
	p.flushTicker.Stop()
	p.wg.Wait()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	s := p.snap.Load()
	if s == nil {
		return nil
	}

	var errs []error
	for _, seg := range s.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// ReadRawMessages snapshots segments without RWMutex so mmap page faults cannot stall appends.
func (p *PartitionLog) ReadRawMessages(startOffset uint64, maxBytes uint32) ([]byte, *[]byte, error) {
	s := p.snap.Load()
	if s == nil || len(s.segments) == 0 {
		return nil, nil, ErrSegmentNotFound
	}

	var targetSeg *Segment
	for i := len(s.segments) - 1; i >= 0; i-- {
		if s.segments[i].baseOffset <= startOffset {
			targetSeg = s.segments[i]
			break
		}
	}

	if targetSeg == nil {
		return nil, nil, ErrSegmentNotFound
	}

	pos, err := targetSeg.FindPosition(startOffset)
	if err != nil {
		return nil, nil, err
	}

	targetPos, _, totalMsgBytes, err := targetSeg.LocateMessages(pos, startOffset, maxBytes)
	if err != nil {
		return nil, nil, err
	}

	if totalMsgBytes == 0 {
		return nil, nil, io.EOF
	}

	var bufPtr *[]byte
	var buf []byte
	if totalMsgBytes <= 1024*1024 {
		bufPtr = FetchBufPool.Get().(*[]byte)
		buf = (*bufPtr)[:totalMsgBytes]
	} else {
		buf = make([]byte, totalMsgBytes)
	}

	copy(buf, targetSeg.mmapData[targetPos:targetPos+int64(totalMsgBytes)])
	return buf, bufPtr, nil
}

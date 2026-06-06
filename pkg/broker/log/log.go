package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var ErrSegmentNotFound = errors.New("segment not found")

type IndexEntry struct {
	Offset   uint64
	Position int64
}

type Segment struct {
	baseOffset uint64
	logFile    *os.File
	indexFile  *os.File
	logPath    string
	indexPath  string
	logSize    int64
	indexSize  int64

	indexCache []IndexEntry

	mmapData   []byte
	maxSegSize int64
}

func NewSegment(dir string, baseOffset uint64, maxSegSize int64, writeable bool) (*Segment, error) {
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
	numEntries := indexSize / 16
	indexCache := make([]IndexEntry, 0, numEntries)

	if numEntries > 0 {
		idxData := make([]byte, numEntries*16)
		if _, err := io.ReadFull(indexFile, idxData); err == nil {
			for i := int64(0); i < numEntries; i++ {
				off := binary.BigEndian.Uint64(idxData[i*16 : i*16+8])
				pos := int64(binary.BigEndian.Uint64(idxData[i*16+8 : i*16+16]))
				indexCache = append(indexCache, IndexEntry{Offset: off, Position: pos})
			}
		}
	}

	var mmapData []byte
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
			return nil, fmt.Errorf("mmap failed: %w", err)
		}
	} else {
		if logSize > 0 {
			mmapData, err = syscall.Mmap(int(logFile.Fd()), 0, int(logSize), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				_ = logFile.Close()
				_ = indexFile.Close()
				return nil, fmt.Errorf("mmap failed: %w", err)
			}
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
		indexCache: indexCache,
		mmapData:   mmapData,
		maxSegSize: maxSegSize,
	}, nil
}

func (s *Segment) Close() error {
	var errs []error
	if len(s.mmapData) > 0 {
		if err := syscall.Munmap(s.mmapData); err != nil {
			errs = append(errs, err)
		}
		s.mmapData = nil
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

func (s *Segment) Write(offset uint64, payload []byte) (int64, error) {
	payloadLen := len(payload)
	length := uint32(8 + payloadLen)
	totalLen := 12 + payloadLen
	pos := s.logSize

	if pos+int64(totalLen) > s.maxSegSize {
		return 0, errors.New("segment space exhausted")
	}

	binary.BigEndian.PutUint32(s.mmapData[pos:pos+4], length)
	binary.BigEndian.PutUint64(s.mmapData[pos+4:pos+12], offset)
	copy(s.mmapData[pos+12:pos+int64(totalLen)], payload)

	s.logSize += int64(totalLen)
	return pos, nil
}

func (s *Segment) WriteIndexEntry(offset uint64, position int64) error {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], offset)
	binary.BigEndian.PutUint64(buf[8:16], uint64(position))

	if _, err := s.indexFile.WriteAt(buf[:], s.indexSize); err != nil {
		return err
	}
	s.indexSize += 16
	s.indexCache = append(s.indexCache, IndexEntry{Offset: offset, Position: position})
	return nil
}

func (s *Segment) FindPosition(offset uint64) (int64, error) {
	n := len(s.indexCache)
	if n == 0 {
		return 0, nil
	}

	low := 0
	high := n - 1
	var bestPos int64 = 0

	for low <= high {
		mid := (low + high) / 2
		entry := s.indexCache[mid]
		if entry.Offset <= offset {
			bestPos = entry.Position
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return bestPos, nil
}

func (s *Segment) Recover() (uint64, error) {
	idxInfo, err := s.indexFile.Stat()
	if err != nil {
		return s.baseOffset, err
	}

	idxSize := idxInfo.Size()
	if idxSize%16 != 0 {
		idxSize -= idxSize % 16
		_ = s.indexFile.Truncate(idxSize)
	}

	var lastIdxOffset uint64 = s.baseOffset
	var lastIdxPos int64 = 0

	if idxSize >= 16 {
		var buf [16]byte
		if _, err := s.indexFile.ReadAt(buf[:], idxSize-16); err == nil {
			lastIdxOffset = binary.BigEndian.Uint64(buf[0:8])
			lastIdxPos = int64(binary.BigEndian.Uint64(buf[8:16]))
		}
	}

	s.indexSize = idxSize

	numEntries := idxSize / 16
	indexCache := make([]IndexEntry, 0, numEntries)
	if numEntries > 0 {
		idxData := make([]byte, numEntries*16)
		if _, err := s.indexFile.ReadAt(idxData, 0); err == nil {
			for i := int64(0); i < numEntries; i++ {
				off := binary.BigEndian.Uint64(idxData[i*16 : i*16+8])
				pos := int64(binary.BigEndian.Uint64(idxData[i*16+8 : i*16+16]))
				indexCache = append(indexCache, IndexEntry{Offset: off, Position: pos})
			}
		}
	}
	s.indexCache = indexCache

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

	s.logSize = currentPos
	if err := s.logFile.Truncate(s.logSize); err != nil {
		return currentOffset, err
	}

	if len(s.mmapData) > 0 {
		_ = syscall.Munmap(s.mmapData)
		s.mmapData = nil
	}

	if mmapSize == s.maxSegSize {
		if err := s.logFile.Truncate(s.maxSegSize); err != nil {
			return currentOffset, err
		}
		s.mmapData, err = syscall.Mmap(int(s.logFile.Fd()), 0, int(s.maxSegSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	} else {
		if s.logSize > 0 {
			s.mmapData, err = syscall.Mmap(int(s.logFile.Fd()), 0, int(s.logSize), syscall.PROT_READ, syscall.MAP_SHARED)
		}
	}
	return currentOffset, err
}

func (s *Segment) LocateMessages(indexPos int64, startOffset uint64, maxBytes uint32) (int64, uint32, uint32, error) {
	logSize := s.logSize
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

type PartitionLog struct {
	mu            sync.RWMutex
	dir           string
	segments      []*Segment
	activeSeg     *Segment
	nextOffset    uint64
	bytesSinceIdx int64
	indexInterval int64
	maxSegSize    int64
	flushTicker   *time.Ticker
	closeChan     chan struct{}
	wg            sync.WaitGroup

	DiskOK atomic.Bool
}

func NewPartitionLog(dir string, maxSegSize int64, indexInterval int64) (*PartitionLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	p := &PartitionLog{
		dir:           dir,
		maxSegSize:    maxSegSize,
		indexInterval: indexInterval,
		closeChan:     make(chan struct{}),
	}
	p.DiskOK.Store(true)

	if err := p.loadSegments(); err != nil {
		return nil, err
	}

	p.startFlushLoop()
	return p, nil
}

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

	for i, offset := range baseOffsets {
		isLast := i == len(baseOffsets)-1
		seg, err := NewSegment(p.dir, offset, p.maxSegSize, isLast)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, seg)
	}

	if len(p.segments) == 0 {
		seg, err := NewSegment(p.dir, 0, p.maxSegSize, true)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, seg)
	}

	p.activeSeg = p.segments[len(p.segments)-1]

	next, err := p.activeSeg.Recover()
	if err != nil {
		return fmt.Errorf("failed to recover active segment: %w", err)
	}
	p.nextOffset = next

	return nil
}

func (p *PartitionLog) startFlushLoop() {
	p.flushTicker = time.NewTicker(100 * time.Millisecond)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.flushTicker.C:
				p.Sync()
			case <-p.closeChan:
				return
			}
		}
	}()
}

func (p *PartitionLog) NextOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextOffset
}

func (p *PartitionLog) Append(payload []byte) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset := p.nextOffset
	totalLen := int64(12 + len(payload))

	if p.activeSeg.logSize+totalLen > p.maxSegSize {
		if err := p.roll(); err != nil {
			return 0, err
		}
	}
	pos, err := p.activeSeg.Write(offset, payload)
	if err != nil {
		return 0, err
	}

	p.nextOffset++
	p.bytesSinceIdx += int64(12 + len(payload))

	if p.bytesSinceIdx >= p.indexInterval {
		if err := p.activeSeg.WriteIndexEntry(offset, pos); err != nil {
			return 0, err
		}
		p.bytesSinceIdx = 0
	}

	return offset, nil
}

func (p *PartitionLog) roll() error {
	if err := p.activeSeg.Sync(); err != nil {
		return err
	}

	if len(p.activeSeg.mmapData) > 0 {
		_ = syscall.Munmap(p.activeSeg.mmapData)
		p.activeSeg.mmapData = nil
	}
	if err := p.activeSeg.logFile.Truncate(p.activeSeg.logSize); err != nil {
		return err
	}
	var err error
	if p.activeSeg.logSize > 0 {
		p.activeSeg.mmapData, err = syscall.Mmap(int(p.activeSeg.logFile.Fd()), 0, int(p.activeSeg.logSize), syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return err
		}
	}

	newSeg, err := NewSegment(p.dir, p.nextOffset, p.maxSegSize, true)
	if err != nil {
		return err
	}

	p.segments = append(p.segments, newSeg)
	p.activeSeg = newSeg
	p.bytesSinceIdx = 0
	return nil
}

func (p *PartitionLog) Sync() {
	p.mu.RLock()
	active := p.activeSeg
	p.mu.RUnlock()

	if active != nil {
		_ = active.Sync()
	}
}

func (p *PartitionLog) Close() error {
	close(p.closeChan)
	p.flushTicker.Stop()
	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	for _, seg := range p.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (p *PartitionLog) ReadRawMessages(startOffset uint64, maxBytes uint32) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.segments) == 0 {
		return nil, ErrSegmentNotFound
	}

	var targetSeg *Segment
	for i := len(p.segments) - 1; i >= 0; i-- {
		if p.segments[i].baseOffset <= startOffset {
			targetSeg = p.segments[i]
			break
		}
	}

	if targetSeg == nil {
		return nil, ErrSegmentNotFound
	}

	pos, err := targetSeg.FindPosition(startOffset)
	if err != nil {
		return nil, err
	}

	targetPos, _, totalMsgBytes, err := targetSeg.LocateMessages(pos, startOffset, maxBytes)
	if err != nil {
		return nil, err
	}

	if totalMsgBytes == 0 {
		return nil, io.EOF
	}

	buf := make([]byte, totalMsgBytes)
	copy(buf, targetSeg.mmapData[targetPos:targetPos+int64(totalMsgBytes)])
	return buf, nil
}

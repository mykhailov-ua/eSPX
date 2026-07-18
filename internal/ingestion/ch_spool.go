package ingestion

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"time"

	"espx/internal/campaignmodel"
	"espx/internal/ingestion/pb"
	"espx/internal/metrics"
)

const (
	chSpoolDefaultSegmentSize = 512 * 1024 * 1024
	chSpoolDefaultMaxSegments = 8
	chSpoolActiveName         = "events.wal"
)

var (
	errCHSpoolFull        = errors.New("ch spool segment full")
	errCHSpoolCorrupt     = errors.New("ch spool record corrupt")
	errCHSpoolNoSpace     = errors.New("ch spool record too large")
	errCHSpoolMaxSegments = errors.New("ch spool max segments exceeded")
	chSpoolRecordMagic    = [4]byte{'C', 'H', 'S', 'P'}
)

// CHSpoolConfig controls mmap segment size and rotation retention.
type CHSpoolConfig struct {
	SegmentSizeBytes int64
	MaxSegments      int
}

// DefaultCHSpoolConfig returns production defaults (512 MiB segment, 8 segments).
func DefaultCHSpoolConfig() CHSpoolConfig {
	return CHSpoolConfig{
		SegmentSizeBytes: chSpoolDefaultSegmentSize,
		MaxSegments:      chSpoolDefaultMaxSegments,
	}
}

// CHSpool is a rotating mmap WAL for ClickHouse batches during outages.
// Only the active write segment keeps an open FD and mmap; sealed segments are lazy-mapped on Scan.
type CHSpool struct {
	dir      string
	cfg      CHSpoolConfig
	mu       sync.Mutex
	active   *chSpoolSegment
	rotated  []string // sealed segment paths, oldest first
	nextSeq  int
	writePos atomic.Int64
}

type chSpoolSegment struct {
	path     string
	file     *os.File
	mmap     []byte
	writePos int64
}

// OpenCHSpool opens the spool directory with production segment defaults.
func OpenCHSpool(dir string) (*CHSpool, error) {
	return OpenCHSpoolWithConfig(dir, DefaultCHSpoolConfig())
}

// OpenCHSpoolWithConfig opens or recovers a multi-segment spool under dir.
func OpenCHSpoolWithConfig(dir string, cfg CHSpoolConfig) (*CHSpool, error) {
	if dir == "" {
		dir = "/var/spool/espx/ch"
	}
	if cfg.SegmentSizeBytes <= 0 {
		cfg.SegmentSizeBytes = chSpoolDefaultSegmentSize
	}
	if cfg.MaxSegments <= 0 {
		cfg.MaxSegments = chSpoolDefaultMaxSegments
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("ch spool mkdir: %w", err)
	}

	spool := &CHSpool{dir: dir, cfg: cfg}
	rotated, nextSeq, err := listCHSpoolRotated(dir)
	if err != nil {
		return nil, err
	}
	spool.rotated = rotated
	spool.nextSeq = nextSeq

	activePath := filepath.Join(dir, chSpoolActiveName)
	seg, err := openCHSpoolSegment(activePath, cfg.SegmentSizeBytes, true)
	if err != nil {
		return nil, err
	}
	spool.active = seg
	spool.writePos.Store(seg.writePos)
	return spool, nil
}

func listCHSpoolRotated(dir string) ([]string, int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	var rotated []string
	maxSeq := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, chSpoolActiveName+".") {
			continue
		}
		suffix := strings.TrimPrefix(name, chSpoolActiveName+".")
		seq, err := strconv.Atoi(suffix)
		if err != nil || seq <= 0 {
			continue
		}
		rotated = append(rotated, filepath.Join(dir, name))
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	sort.Slice(rotated, func(i, j int) bool {
		return chSpoolSeqFromPath(rotated[i]) < chSpoolSeqFromPath(rotated[j])
	})
	return rotated, maxSeq + 1, nil
}

func chSpoolSeqFromPath(path string) int {
	base := filepath.Base(path)
	suffix := strings.TrimPrefix(base, chSpoolActiveName+".")
	seq, _ := strconv.Atoi(suffix)
	return seq
}

func openCHSpoolSegment(path string, segmentSize int64, create bool) (*chSpoolSegment, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(path, flags, 0o640)
	if err != nil {
		return nil, fmt.Errorf("ch spool open %s: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	size := info.Size()
	if size > segmentSize {
		_ = file.Close()
		return nil, fmt.Errorf("ch spool segment %s exceeds max size: %d", path, size)
	}
	if size == 0 {
		if err := file.Truncate(segmentSize); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("ch spool truncate: %w", err)
		}
		size = segmentSize
	} else if size < segmentSize {
		if err := file.Truncate(segmentSize); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("ch spool grow: %w", err)
		}
		size = segmentSize
	}

	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("ch spool mmap: %w", err)
	}

	writePos := findCHSpoolWritePos(mmap)
	return &chSpoolSegment{
		path:     path,
		file:     file,
		mmap:     mmap,
		writePos: writePos,
	}, nil
}

func mapCHSpoolSegmentReadOnly(path string, segmentSize int64) (*chSpoolSegment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		_ = file.Close()
		return &chSpoolSegment{path: path, writePos: 0}, nil
	}
	if size > segmentSize {
		size = segmentSize
	}
	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &chSpoolSegment{
		path:     path,
		file:     file,
		mmap:     mmap,
		writePos: findCHSpoolWritePos(mmap),
	}, nil
}

func closeCHSpoolSegment(seg *chSpoolSegment) error {
	if seg == nil {
		return nil
	}
	if len(seg.mmap) > 0 {
		_ = syscall.Munmap(seg.mmap)
		seg.mmap = nil
	}
	if seg.file != nil {
		err := seg.file.Close()
		seg.file = nil
		return err
	}
	return nil
}

// findCHSpoolWritePos scans valid records and returns the append offset after the last record.
func findCHSpoolWritePos(data []byte) int64 {
	var pos int64
	for pos+12 <= int64(len(data)) {
		if !bytesEqual4(data[pos:pos+4], chSpoolRecordMagic[:]) {
			break
		}
		recordLen := int64(binary.BigEndian.Uint32(data[pos+4 : pos+8]))
		if recordLen < 12 || pos+recordLen > int64(len(data)) {
			break
		}
		payloadLen := int(recordLen - 12)
		if payloadLen > 0 {
			want := binary.BigEndian.Uint32(data[pos+8 : pos+12])
			got := crc32.ChecksumIEEE(data[pos+12 : pos+recordLen])
			if want != got {
				break
			}
		}
		pos += recordLen
	}
	return pos
}

func bytesEqual4(a, b []byte) bool {
	return len(a) >= 4 && len(b) >= 4 && a[0] == b[0] && a[1] == b[1] && a[2] == b[2] && a[3] == b[3]
}

// AppendDurably writes one batch to the active segment, rotates on full, and fsyncs before return.
func (s *CHSpool) AppendDurably(dedupToken string, events []*campaignmodel.Event) error {
	if len(events) == 0 {
		return nil
	}
	payload, err := marshalCHSpoolPayload(dedupToken, events)
	if err != nil {
		return err
	}
	recordLen := 12 + len(payload)
	if recordLen > 16*1024*1024 {
		return errCHSpoolNoSpace
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.appendLocked(payload, recordLen); err != nil {
		if !errors.Is(err, errCHSpoolFull) {
			return err
		}
		if rotErr := s.rotateLocked(); rotErr != nil {
			return rotErr
		}
		metrics.CHSpoolRotateTotal.Inc()
		return s.appendLocked(payload, recordLen)
	}
	return nil
}

func (s *CHSpool) appendLocked(payload []byte, recordLen int) error {
	seg := s.active
	pos := seg.writePos
	if pos+int64(recordLen) > int64(len(seg.mmap)) {
		return errCHSpoolFull
	}

	copy(seg.mmap[pos:pos+4], chSpoolRecordMagic[:])
	binary.BigEndian.PutUint32(seg.mmap[pos+4:pos+8], uint32(recordLen))
	binary.BigEndian.PutUint32(seg.mmap[pos+8:pos+12], crc32.ChecksumIEEE(payload))
	copy(seg.mmap[pos+12:pos+int64(recordLen)], payload)

	if err := seg.file.Sync(); err != nil {
		return fmt.Errorf("ch spool fsync: %w", err)
	}

	seg.writePos = pos + int64(recordLen)
	s.writePos.Store(seg.writePos)
	return nil
}

// rotateLocked seals the active segment, opens a fresh one, and enforces MaxSegments.
func (s *CHSpool) rotateLocked() error {
	total := len(s.rotated) + 1
	if total >= s.cfg.MaxSegments {
		return errCHSpoolMaxSegments
	}

	if err := s.active.file.Sync(); err != nil {
		return fmt.Errorf("ch spool sync before rotate: %w", err)
	}

	sealedPath := filepath.Join(s.dir, fmt.Sprintf("%s.%04d", chSpoolActiveName, s.nextSeq))
	if err := os.Rename(s.active.path, sealedPath); err != nil {
		return fmt.Errorf("ch spool rotate rename: %w", err)
	}
	if err := closeCHSpoolSegment(s.active); err != nil {
		return err
	}
	s.rotated = append(s.rotated, sealedPath)
	s.nextSeq++

	activePath := filepath.Join(s.dir, chSpoolActiveName)
	seg, err := openCHSpoolSegment(activePath, s.cfg.SegmentSizeBytes, true)
	if err != nil {
		return err
	}
	s.active = seg
	s.writePos.Store(seg.writePos)
	return nil
}

// marshalCHSpoolPayload encodes dedup token and vtproto events for WAL recovery.
func marshalCHSpoolPayload(dedupToken string, events []*campaignmodel.Event) ([]byte, error) {
	tokenBytes := []byte(dedupToken)
	if len(tokenBytes) > 0xffff {
		return nil, fmt.Errorf("dedup token too long")
	}

	var total int
	sizes := make([]int, len(events))
	for i, e := range events {
		pbEvt := eventToStreamPB(e)
		n := pbEvt.SizeVT()
		sizes[i] = n
		total += 4 + n
		DeepResetAdStreamEvent(pbEvt)
		streamEventPool.Put(pbEvt)
	}

	buf := make([]byte, 2+len(tokenBytes)+4+total)
	off := 0
	binary.BigEndian.PutUint16(buf[off:], uint16(len(tokenBytes)))
	off += 2
	copy(buf[off:], tokenBytes)
	off += len(tokenBytes)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(events)))
	off += 4

	for i, e := range events {
		pbEvt := eventToStreamPB(e)
		n, err := pbEvt.MarshalToSizedBufferVT(buf[off+4 : off+4+sizes[i]])
		if err != nil {
			DeepResetAdStreamEvent(pbEvt)
			streamEventPool.Put(pbEvt)
			return nil, err
		}
		binary.BigEndian.PutUint32(buf[off:], uint32(n))
		off += 4 + n
		DeepResetAdStreamEvent(pbEvt)
		streamEventPool.Put(pbEvt)
	}
	return buf, nil
}

func eventToStreamPB(e *campaignmodel.Event) *pb.AdStreamEvent {
	pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
	DeepResetAdStreamEvent(pbEvt)
	pbEvt.ClickId = append(pbEvt.ClickId[:0], e.ClickID...)
	pbEvt.CampaignId = append(pbEvt.CampaignId[:0], e.CampaignID[:]...)
	pbEvt.EventType = append(pbEvt.EventType[:0], e.Type...)
	pbEvt.Payload = append(pbEvt.Payload[:0], e.Payload...)
	pbEvt.Ip = append(pbEvt.Ip[:0], e.IP...)
	pbEvt.Ua = append(pbEvt.Ua[:0], e.UA...)
	pbEvt.FraudReason = append(pbEvt.FraudReason[:0], e.FraudReason...)
	pbEvt.FraudScore = e.FraudScore
	pbEvt.GhostEvent = e.GhostEvent
	if !e.CreatedAt.IsZero() {
		pbEvt.CreatedAtUnix = e.CreatedAt.Unix()
	}
	return pbEvt
}

// CHSpoolRecord is one recovered WAL entry.
type CHSpoolRecord struct {
	DedupToken    string
	Events        []*campaignmodel.Event
	SegmentPath   string
	EndOffset     int64
	LastInSegment bool
}

// Scan returns all valid records across sealed and active segments in append order.
func (s *CHSpool) Scan() ([]CHSpoolRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []CHSpoolRecord
	paths := append(append([]string{}, s.rotated...), s.active.path)
	for _, path := range paths {
		records, err := s.scanPathLocked(path)
		if err != nil {
			return out, err
		}
		out = append(out, records...)
	}
	return out, nil
}

func (s *CHSpool) scanPathLocked(path string) ([]CHSpoolRecord, error) {
	var seg *chSpoolSegment
	var err error
	lazy := path != s.active.path
	if lazy {
		seg, err = mapCHSpoolSegmentReadOnly(path, s.cfg.SegmentSizeBytes)
		if err != nil {
			return nil, err
		}
		defer func() { _ = closeCHSpoolSegment(seg) }()
	} else {
		seg = s.active
	}

	var out []CHSpoolRecord
	pos := int64(0)
	data := seg.mmap
	for pos+12 <= int64(len(data)) {
		if !bytesEqual4(data[pos:pos+4], chSpoolRecordMagic[:]) {
			break
		}
		recordLen := int64(binary.BigEndian.Uint32(data[pos+4 : pos+8]))
		if recordLen < 12 || pos+recordLen > int64(len(data)) {
			return out, errCHSpoolCorrupt
		}
		payload := data[pos+12 : pos+recordLen]
		want := binary.BigEndian.Uint32(data[pos+8 : pos+12])
		if crc32.ChecksumIEEE(payload) != want {
			return out, errCHSpoolCorrupt
		}
		token, events, err := unmarshalCHSpoolPayload(payload)
		if err != nil {
			return out, err
		}
		end := pos + recordLen
		out = append(out, CHSpoolRecord{
			DedupToken:    token,
			Events:        events,
			SegmentPath:   path,
			EndOffset:     end,
			LastInSegment: end == seg.writePos,
		})
		pos += recordLen
	}
	return out, nil
}

func unmarshalCHSpoolPayload(payload []byte) (string, []*campaignmodel.Event, error) {
	if len(payload) < 6 {
		return "", nil, errCHSpoolCorrupt
	}
	tokenLen := int(binary.BigEndian.Uint16(payload[0:2]))
	if len(payload) < 2+tokenLen+4 {
		return "", nil, errCHSpoolCorrupt
	}
	token := string(payload[2 : 2+tokenLen])
	off := 2 + tokenLen
	count := int(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4

	events := make([]*campaignmodel.Event, 0, count)
	for i := 0; i < count; i++ {
		if off+4 > len(payload) {
			return "", nil, errCHSpoolCorrupt
		}
		n := int(binary.BigEndian.Uint32(payload[off : off+4]))
		off += 4
		if n < 0 || off+n > len(payload) {
			return "", nil, errCHSpoolCorrupt
		}
		pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
		DeepResetAdStreamEvent(pbEvt)
		if err := pbEvt.UnmarshalVT(payload[off : off+n]); err != nil {
			DeepResetAdStreamEvent(pbEvt)
			streamEventPool.Put(pbEvt)
			return "", nil, err
		}
		off += n
		evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
		evt.Reset()
		evt.ClickID = string(append([]byte(nil), pbEvt.ClickId...))
		_ = ParseUUID(pbEvt.CampaignId, &evt.CampaignID)
		evt.Type = string(append([]byte(nil), pbEvt.EventType...))
		evt.Payload = append(evt.Payload[:0], pbEvt.Payload...)
		evt.IP = string(append([]byte(nil), pbEvt.Ip...))
		evt.UA = string(append([]byte(nil), pbEvt.Ua...))
		evt.FraudReason = string(append([]byte(nil), pbEvt.FraudReason...))
		evt.FraudScore = pbEvt.FraudScore
		evt.GhostEvent = pbEvt.GhostEvent
		if pbEvt.CreatedAtUnix > 0 {
			evt.CreatedAt = time.Unix(pbEvt.CreatedAtUnix, 0).UTC()
		}
		DeepResetAdStreamEvent(pbEvt)
		streamEventPool.Put(pbEvt)
		events = append(events, evt)
	}
	return token, events, nil
}

// ReleaseRecord drops a replayed record: compacts sealed/active segments and deletes empty sealed files.
func (s *CHSpool) ReleaseRecord(rec CHSpoolRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rec.SegmentPath == s.active.path {
		return s.truncateActiveLocked(rec.EndOffset)
	}

	seg, err := openCHSpoolSegment(rec.SegmentPath, s.cfg.SegmentSizeBytes, false)
	if err != nil {
		return err
	}
	defer func() { _ = closeCHSpoolSegment(seg) }()

	if rec.EndOffset >= seg.writePos {
		return s.removeRotatedLocked(rec.SegmentPath)
	}

	remaining := seg.writePos - rec.EndOffset
	copy(seg.mmap, seg.mmap[rec.EndOffset:seg.writePos])
	for i := remaining; i < int64(len(seg.mmap)); i++ {
		seg.mmap[i] = 0
	}
	seg.writePos = remaining
	if err := seg.file.Sync(); err != nil {
		return err
	}
	if seg.writePos == 0 {
		return s.removeRotatedLocked(rec.SegmentPath)
	}
	return nil
}

func (s *CHSpool) removeRotatedLocked(path string) error {
	for i, p := range s.rotated {
		if p == path {
			s.rotated = append(s.rotated[:i], s.rotated[i+1:]...)
			return os.Remove(path)
		}
	}
	return os.Remove(path)
}

// TruncatePrefix compacts the active segment after replay (legacy API; prefer ReleaseRecord).
func (s *CHSpool) TruncatePrefix(endOffset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.truncateActiveLocked(endOffset)
}

func (s *CHSpool) truncateActiveLocked(endOffset int64) error {
	if endOffset <= 0 || endOffset > s.active.writePos {
		return nil
	}

	remaining := s.active.writePos - endOffset
	copy(s.active.mmap, s.active.mmap[endOffset:s.active.writePos])
	for i := remaining; i < int64(len(s.active.mmap)); i++ {
		s.active.mmap[i] = 0
	}
	s.active.writePos = remaining
	s.writePos.Store(remaining)
	if err := s.active.file.Truncate(s.cfg.SegmentSizeBytes); err != nil {
		return err
	}
	return s.active.file.Sync()
}

// SegmentCount returns sealed segments plus the active writer (for chaos diagnostics).
func (s *CHSpool) SegmentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rotated) + 1
}

// OpenFDCount returns 1 when the active segment FD is open, 0 after Close.
func (s *CHSpool) OpenFDCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil && s.active.file != nil {
		return 1
	}
	return 0
}

// Close unmmaps the active segment and releases its file descriptor.
func (s *CHSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return closeCHSpoolSegment(s.active)
}

// WritePos exposes the durable append offset on the active segment for tests.
func (s *CHSpool) WritePos() int64 {
	return s.writePos.Load()
}

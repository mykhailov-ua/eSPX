package licensing

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const (
	licenseSpoolFileName        = "license.wal"
	licenseSpoolDefaultSegment  = 64 * 1024
	licenseSpoolDefaultMaxToken = 16 * 1024
	licenseSpoolRecordHeader    = 12
)

var (
	licenseSpoolMagic       = [4]byte{'L', 'C', 'S', 'H'}
	ErrLicenseSpoolFull     = errors.New("license spool segment full")
	ErrLicenseSpoolCorrupt  = errors.New("license spool record corrupt")
	ErrLicenseTokenTooLarge = errors.New("license token exceeds max size")
)

// LicenseSpoolConfig controls mmap segment sizing and token caps.
type LicenseSpoolConfig struct {
	SegmentSizeBytes int64
	MaxTokenBytes    int
}

// DefaultLicenseSpoolConfig returns page-aligned defaults for cold-path durability.
func DefaultLicenseSpoolConfig() LicenseSpoolConfig {
	return LicenseSpoolConfig{
		SegmentSizeBytes: alignToPageSize(licenseSpoolDefaultSegment),
		MaxTokenBytes:    licenseSpoolDefaultMaxToken,
	}
}

// LicenseSpool is a single-segment mmap WAL for license JWT durability between heartbeat and file cache.
type LicenseSpool struct {
	dir string
	cfg LicenseSpoolConfig
	mu  sync.Mutex
	seg *licenseSpoolSegment
}

type licenseSpoolSegment struct {
	path     string
	file     *os.File
	mmap     []byte
	writePos int64
}

// OpenLicenseSpool opens or recovers the license WAL under dir.
func OpenLicenseSpool(dir string) (*LicenseSpool, error) {
	return OpenLicenseSpoolWithConfig(dir, DefaultLicenseSpoolConfig())
}

// OpenLicenseSpoolWithConfig opens a WAL with explicit sizing. Segment size is rounded up to OS page size.
func OpenLicenseSpoolWithConfig(dir string, cfg LicenseSpoolConfig) (*LicenseSpool, error) {
	if dir == "" {
		return nil, errors.New("license spool dir is required")
	}
	if cfg.SegmentSizeBytes <= 0 {
		cfg.SegmentSizeBytes = alignToPageSize(licenseSpoolDefaultSegment)
	} else {
		cfg.SegmentSizeBytes = alignToPageSize(cfg.SegmentSizeBytes)
	}
	if cfg.MaxTokenBytes <= 0 {
		cfg.MaxTokenBytes = licenseSpoolDefaultMaxToken
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("license spool mkdir: %w", err)
	}

	path := filepath.Join(dir, licenseSpoolFileName)
	seg, err := openLicenseSpoolSegment(path, cfg.SegmentSizeBytes)
	if err != nil {
		return nil, err
	}
	return &LicenseSpool{dir: dir, cfg: cfg, seg: seg}, nil
}

func alignToPageSize(size int64) int64 {
	page := int64(syscall.Getpagesize())
	if page <= 0 {
		page = 4096
	}
	return ((size + page - 1) / page) * page
}

func openLicenseSpoolSegment(path string, segmentSize int64) (*licenseSpoolSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return nil, fmt.Errorf("license spool open %s: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	size := info.Size()
	if size > segmentSize {
		_ = file.Close()
		return nil, fmt.Errorf("license spool %s exceeds max size %d", path, segmentSize)
	}
	if size == 0 {
		if err := file.Truncate(segmentSize); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("license spool truncate: %w", err)
		}
		size = segmentSize
	} else if size < segmentSize {
		if err := file.Truncate(segmentSize); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("license spool grow: %w", err)
		}
		size = segmentSize
	}

	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("license spool mmap: %w", err)
	}

	writePos := findLicenseSpoolWritePos(mmap)
	return &licenseSpoolSegment{path: path, file: file, mmap: mmap, writePos: writePos}, nil
}

func findLicenseSpoolWritePos(data []byte) int64 {
	var pos int64
	for pos+licenseSpoolRecordHeader <= int64(len(data)) {
		if !licenseSpoolMagicEqual(data[pos : pos+4]) {
			break
		}
		recordLen := int64(binary.BigEndian.Uint32(data[pos+4 : pos+8]))
		if recordLen < licenseSpoolRecordHeader || pos+recordLen > int64(len(data)) {
			break
		}
		payloadLen := int(recordLen - licenseSpoolRecordHeader)
		if payloadLen > 0 {
			want := binary.BigEndian.Uint32(data[pos+8 : pos+12])
			got := crc32.ChecksumIEEE(data[pos+licenseSpoolRecordHeader : pos+recordLen])
			if want != got {
				break
			}
		}
		pos += recordLen
	}
	return pos
}

func licenseSpoolMagicEqual(b []byte) bool {
	return len(b) >= 4 &&
		b[0] == licenseSpoolMagic[0] &&
		b[1] == licenseSpoolMagic[1] &&
		b[2] == licenseSpoolMagic[2] &&
		b[3] == licenseSpoolMagic[3]
}

// AppendDurably appends a JWT token and fsyncs before return.
func (s *LicenseSpool) AppendDurably(token string) error {
	if len(token) > s.cfg.MaxTokenBytes {
		return ErrLicenseTokenTooLarge
	}
	payload := []byte(token)
	recordLen := licenseSpoolRecordHeader + len(payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendLocked(payload, recordLen, true)
}

func (s *LicenseSpool) appendLocked(payload []byte, recordLen int, fsync bool) error {
	seg := s.seg
	pos := seg.writePos
	if pos+int64(recordLen) > int64(len(seg.mmap)) {
		return ErrLicenseSpoolFull
	}

	copy(seg.mmap[pos:pos+4], licenseSpoolMagic[:])
	binary.BigEndian.PutUint32(seg.mmap[pos+4:pos+8], uint32(recordLen))
	binary.BigEndian.PutUint32(seg.mmap[pos+8:pos+12], crc32.ChecksumIEEE(payload))
	copy(seg.mmap[pos+licenseSpoolRecordHeader:pos+int64(recordLen)], payload)

	if fsync {
		if err := seg.file.Sync(); err != nil {
			return fmt.Errorf("license spool fsync: %w", err)
		}
	}

	seg.writePos = pos + int64(recordLen)
	return nil
}

// LatestToken returns the most recent durable token or empty string if none.
func (s *LicenseSpool) LatestToken() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tokens, err := s.scanLocked()
	if err != nil {
		return "", err
	}
	if len(tokens) == 0 {
		return "", nil
	}
	return tokens[len(tokens)-1], nil
}

// Recover replays all valid records from the WAL.
func (s *LicenseSpool) Recover() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanLocked()
}

func (s *LicenseSpool) scanLocked() ([]string, error) {
	var tokens []string
	pos := int64(0)
	data := s.seg.mmap
	for pos+licenseSpoolRecordHeader <= int64(len(data)) {
		if !licenseSpoolMagicEqual(data[pos : pos+4]) {
			break
		}
		recordLen := int64(binary.BigEndian.Uint32(data[pos+4 : pos+8]))
		if recordLen < licenseSpoolRecordHeader || pos+recordLen > int64(len(data)) {
			break
		}
		payload := data[pos+licenseSpoolRecordHeader : pos+recordLen]
		want := binary.BigEndian.Uint32(data[pos+8 : pos+12])
		got := crc32.ChecksumIEEE(payload)
		if want != got {
			break
		}
		tokens = append(tokens, string(payload))
		pos += recordLen
	}
	s.seg.writePos = pos
	return tokens, nil
}

// Close unmaps and closes the active segment.
func (s *LicenseSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seg == nil {
		return nil
	}
	if len(s.seg.mmap) > 0 {
		_ = syscall.Munmap(s.seg.mmap)
		s.seg.mmap = nil
	}
	if s.seg.file != nil {
		err := s.seg.file.Close()
		s.seg.file = nil
		return err
	}
	return nil
}

// SegmentSize reports the page-aligned mmap size (for diagnostics).
func (s *LicenseSpool) SegmentSize() int64 {
	return s.cfg.SegmentSizeBytes
}

// WritePos returns the current append offset.
func (s *LicenseSpool) WritePos() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seg == nil {
		return 0
	}
	return s.seg.writePos
}

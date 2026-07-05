package logcompactor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const maxRecordBytes = 1 << 20

// filterSegmentStream scans length-prefixed records with key-aware impression compaction.
func filterSegmentStream(r io.Reader, sampleRate uint64, w io.Writer) (compactStats, error) {
	kc := newKeyCompactor(sampleRate, w)
	var hdr [4]byte
	recordBuf := make([]byte, 0, 4096)

	for {
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return kc.stats, err
		}

		length := binary.BigEndian.Uint32(hdr[:])
		if length == 0 {
			continue
		}
		if length > maxRecordBytes {
			return kc.stats, fmt.Errorf("%w: %d bytes", ErrRecordTooLarge, length)
		}
		if int(length) > cap(recordBuf) {
			recordBuf = make([]byte, length)
		}
		record := recordBuf[:length]
		if _, err := io.ReadFull(r, record); err != nil {
			return kc.stats, err
		}

		if err := kc.ingest(length, record); err != nil {
			return kc.stats, err
		}
	}

	if kc.stats.OriginalCount == 0 {
		return kc.stats, ErrEmptySegment
	}
	if err := kc.flush(); err != nil {
		return kc.stats, err
	}
	return kc.stats, nil
}

// filterSegment scans an in-memory segment using the same key-aware compaction path.
func filterSegment(src []byte, sampleRate uint64, dst io.Writer) (compactStats, error) {
	return filterSegmentStream(bytes.NewReader(src), sampleRate, dst)
}

// countSegmentRecords counts length-prefixed protobuf records in a plaintext segment stream.
func countSegmentRecords(r io.Reader) (int64, error) {
	var count int64
	var hdr [4]byte

	for {
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}

		length := binary.BigEndian.Uint32(hdr[:])
		if length == 0 {
			continue
		}
		if length > maxRecordBytes {
			return count, fmt.Errorf("%w: %d bytes", ErrRecordTooLarge, length)
		}
		if _, err := io.CopyN(io.Discard, r, int64(length)); err != nil {
			return count, err
		}
		count++
	}
}

// verifyPlaintextSegment checks that a filtered plaintext file contains expectKept records.
func verifyPlaintextSegment(path string, expectKept int64) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	got, err := countSegmentRecords(file)
	if err != nil {
		return err
	}
	if got != expectKept {
		return fmt.Errorf("%w: got %d want %d", ErrVerifyRecordCount, got, expectKept)
	}
	return nil
}

package logcompactor

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"espx/internal/ads/pb"
)

const compactOkSuffix = ".compact.ok"

// CompactMarker documents warm-tier completion for pipeline ordering with log-evacuator.
type CompactMarker struct {
	SourceKey    string `json:"source_key"`
	SourceSHA256 string `json:"source_sha256"`
	DestKey      string `json:"dest_key"`
	DestSHA256   string `json:"dest_sha256"`
	KeptCount    int64  `json:"kept_count"`
}

type queuedRecord struct {
	length uint32
	data   []byte
}

type keyCompactor struct {
	sampleRate  uint64
	impressions map[string]queuedRecord
	w           io.Writer
	stats       compactStats
	evt         pb.AdStreamEvent
}

func newKeyCompactor(sampleRate uint64, w io.Writer) *keyCompactor {
	return &keyCompactor{
		sampleRate:  sampleRate,
		impressions: make(map[string]queuedRecord),
		w:           w,
	}
}

func (kc *keyCompactor) ingest(length uint32, record []byte) error {
	kc.stats.OriginalCount++

	kc.evt = pb.AdStreamEvent{}
	if err := kc.evt.UnmarshalVT(record); err != nil {
		return nil
	}
	if isAlwaysKeepEvent(&kc.evt) {
		return kc.writeRecord(length, record)
	}
	if len(kc.evt.ClickId) == 0 {
		return nil
	}
	if string(kc.evt.EventType) != "impression" {
		return nil
	}

	clickID := string(kc.evt.ClickId)
	kc.impressions[clickID] = queuedRecord{
		length: length,
		data:   append([]byte(nil), record...),
	}
	return nil
}

func (kc *keyCompactor) flush() error {
	for clickID, rec := range kc.impressions {
		if !shouldSampleImpression([]byte(clickID), kc.sampleRate) {
			continue
		}
		if err := kc.writeRecord(rec.length, rec.data); err != nil {
			return err
		}
	}
	return nil
}

func (kc *keyCompactor) writeRecord(length uint32, record []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], length)
	if _, err := kc.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := kc.w.Write(record); err != nil {
		return err
	}
	kc.stats.KeptCount++
	return nil
}

func compactMarkerFileName(hotKey string) string {
	if strings.HasSuffix(hotKey, readySuffix) {
		return strings.TrimSuffix(hotKey, readySuffix) + compactOkSuffix
	}
	if strings.HasSuffix(hotKey, ".log") {
		return strings.TrimSuffix(hotKey, ".log") + compactOkSuffix
	}
	return hotKey + compactOkSuffix
}

// CompactMarkerPath returns the marker file path for a hot segment key in sourceDir.
func CompactMarkerPath(sourceDir, hotKey string) string {
	return filepath.Join(sourceDir, compactMarkerFileName(hotKey))
}

// CompactMarkerReady reports whether compactor finished for a ready segment path.
func CompactMarkerReady(readyPath string) bool {
	markerPath := markerPathFromReady(readyPath)
	_, err := os.Stat(markerPath)
	return err == nil
}

func markerPathFromReady(readyPath string) string {
	return filepath.Join(filepath.Dir(readyPath), compactMarkerFileName(filepath.Base(readyPath)))
}

// WriteCompactMarker persists a completion marker for log-evacuator ordering.
func WriteCompactMarker(sourceDir string, record CheckpointRecord) error {
	marker := CompactMarker{
		SourceKey:    record.SourceKey,
		SourceSHA256: record.SourceSHA256,
		DestKey:      record.DestKey,
		DestSHA256:   record.DestSHA256,
		KeptCount:    record.KeptCount,
	}
	payload, err := json.Marshal(marker)
	if err != nil {
		return err
	}

	path := CompactMarkerPath(sourceDir, record.SourceKey)
	tmpPath := path + warmTmpSuffix
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// RemoveCompactMarker deletes the pipeline marker for a hot segment key.
func RemoveCompactMarker(sourceDir, hotKey string) error {
	err := os.Remove(CompactMarkerPath(sourceDir, hotKey))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func readCompactMarker(path string) (CompactMarker, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return CompactMarker{}, err
	}
	var marker CompactMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return CompactMarker{}, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
	}
	return marker, nil
}

package logcompactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// CompactionMeta is written alongside warm-tier output for ops inspection.
type CompactionMeta struct {
	SourceKey     string    `json:"source_key"`
	DestKey       string    `json:"dest_key"`
	SourceSHA256  string    `json:"source_sha256"`
	DestSHA256    string    `json:"dest_sha256"`
	OriginalCount int64     `json:"original_count"`
	KeptCount     int64     `json:"kept_count"`
	SampleRate    uint64    `json:"sample_rate"`
	CompactedAt   time.Time `json:"compacted_at"`
}

// TierObject describes one hot-tier segment eligible for compaction.
type TierObject struct {
	Key     string
	Path    string
	ModTime time.Time
	Size    int64
}

// TierStore lists hot segments and writes warm-tier compacted output.
type TierStore interface {
	ListHot(ctx context.Context, olderThan time.Time) ([]TierObject, error)
	WriteWarm(ctx context.Context, destKey string, plaintext []byte, meta CompactionMeta) error
	RemoveHot(ctx context.Context, obj TierObject) error
}

// LocalTierStore implements TierStore on a local filesystem (MVP default).
type LocalTierStore struct {
	SourceDir string
	WarmDir   string
}

// NewLocalTierStore returns a filesystem-backed tier store.
func NewLocalTierStore(sourceDir, warmDir string) *LocalTierStore {
	return &LocalTierStore{
		SourceDir: sourceDir,
		WarmDir:   warmDir,
	}
}

// ListHot returns rotated segments in SourceDir older than the cutoff time.
func (store *LocalTierStore) ListHot(_ context.Context, olderThan time.Time) ([]TierObject, error) {
	entries, err := os.ReadDir(store.SourceDir)
	if err != nil {
		return nil, err
	}

	var objects []TierObject
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isHotSegmentName(name) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}
		if !info.ModTime().Before(olderThan) {
			continue
		}

		objects = append(objects, TierObject{
			Key:     name,
			Path:    filepath.Join(store.SourceDir, name),
			ModTime: info.ModTime(),
			Size:    info.Size(),
		})
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].ModTime.Before(objects[j].ModTime)
	})
	return objects, nil
}

func isHotSegmentName(name string) bool {
	if strings.HasSuffix(name, compactingSuffix) {
		return false
	}
	if strings.HasSuffix(name, evacuatingSuffix) {
		return false
	}
	if strings.HasSuffix(name, readySuffix) {
		return true
	}
	return strings.HasPrefix(name, "segment_") && strings.HasSuffix(name, ".log")
}

// WriteWarm zstd-compresses plaintext and writes destKey plus a JSON sidecar.
func (store *LocalTierStore) WriteWarm(_ context.Context, destKey string, plaintext []byte, meta CompactionMeta) error {
	_, err := store.writeWarmFromPath(destKey, bytes.NewReader(plaintext), meta)
	return err
}

// WriteWarmFromFile zstd-compresses a filtered plaintext file with verify-before-finalize semantics.
func (store *LocalTierStore) WriteWarmFromFile(_ context.Context, destKey, filteredPath string, meta CompactionMeta) (string, error) {
	file, err := os.Open(filteredPath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	return store.writeWarmFromPath(destKey, file, meta)
}

func (store *LocalTierStore) writeWarmFromPath(destKey string, plaintext io.Reader, meta CompactionMeta) (string, error) {
	if err := os.MkdirAll(store.WarmDir, 0o755); err != nil {
		return "", err
	}

	destPath := filepath.Join(store.WarmDir, destKey)
	tmpPath := destPath + warmTmpSuffix
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}

	enc, err := zstd.NewWriter(tmpFile, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if _, err := io.Copy(enc, plaintext); err != nil {
		_ = enc.Close()
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := enc.Close(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}

	digest, err := computeFileDigest(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}

	metaPath := strings.TrimSuffix(destPath, ".zst") + ".meta.json"
	meta.DestSHA256 = digest.SHA256
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return "", err
	}
	metaTmp := metaPath + warmTmpSuffix
	if err := os.WriteFile(metaTmp, metaBytes, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(metaTmp, metaPath); err != nil {
		_ = os.Remove(metaTmp)
		return "", err
	}

	return digest.SHA256, nil
}

// RemoveWarmArtifacts deletes incomplete warm-tier temp files for destKey.
func (store *LocalTierStore) RemoveWarmArtifacts(destKey string) {
	destPath := filepath.Join(store.WarmDir, destKey)
	_ = os.Remove(destPath + warmTmpSuffix)
	_ = os.Remove(strings.TrimSuffix(destPath, ".zst") + ".meta.json" + warmTmpSuffix)
	_ = os.Remove(destPath)
	_ = os.Remove(strings.TrimSuffix(destPath, ".zst") + ".meta.json")
}

// RemoveHot deletes a compacted source segment from the hot directory.
func (store *LocalTierStore) RemoveHot(_ context.Context, obj TierObject) error {
	if obj.Path == "" {
		return fmt.Errorf("empty hot object path")
	}
	return os.Remove(obj.Path)
}

// ReadWarm decompresses a warm-tier segment for tests and ops tooling.
func ReadWarm(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return dec.DecodeAll(data, nil)
}

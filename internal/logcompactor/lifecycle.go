package logcompactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	compactingSuffix = ".compacting"
	evacuatingSuffix = ".log.zst.evacuating"
	warmTmpSuffix    = ".tmp"
	filteredTmpExt   = ".filtered.tmp"
)

// hotKeyFromCompacting derives the original hot segment name from a claimed path.
func hotKeyFromCompacting(name string) string {
	if !strings.HasSuffix(name, compactingSuffix) {
		return name
	}
	base := strings.TrimSuffix(name, compactingSuffix)
	if strings.HasSuffix(base, ".log.zst") {
		return base + ".ready"
	}
	return base
}

// compactingPathFor returns the claimed path for a hot segment file.
func compactingPathFor(hotPath string) string {
	if strings.HasSuffix(hotPath, readySuffix) {
		return strings.TrimSuffix(hotPath, readySuffix) + compactingSuffix
	}
	return hotPath + compactingSuffix
}

// ClaimHot renames a hot segment into the compacting state so concurrent workers skip it.
func (store *LocalTierStore) ClaimHot(_ context.Context, obj TierObject) (TierObject, error) {
	dstPath := compactingPathFor(obj.Path)
	if err := os.Rename(obj.Path, dstPath); err != nil {
		if os.IsNotExist(err) {
			return TierObject{}, ErrHotSegmentNotFound
		}
		return TierObject{}, err
	}
	info, err := os.Stat(dstPath)
	if err != nil {
		return TierObject{}, err
	}
	return TierObject{
		Key:     filepath.Base(dstPath),
		Path:    dstPath,
		ModTime: info.ModTime(),
		Size:    info.Size(),
	}, nil
}

// RollbackHot restores a claimed segment to its original hot name after a failed compaction.
func (store *LocalTierStore) RollbackHot(_ context.Context, obj TierObject) error {
	if !strings.HasSuffix(obj.Path, compactingSuffix) {
		return nil
	}
	hotName := hotKeyFromCompacting(filepath.Base(obj.Path))
	hotPath := filepath.Join(store.SourceDir, hotName)
	if err := os.Rename(obj.Path, hotPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ListStuckCompacting returns claimed segments left by a crash mid-compaction.
func (store *LocalTierStore) ListStuckCompacting(_ context.Context) ([]TierObject, error) {
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
		if !strings.HasSuffix(name, compactingSuffix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		objects = append(objects, TierObject{
			Key:     name,
			Path:    filepath.Join(store.SourceDir, name),
			ModTime: info.ModTime(),
			Size:    info.Size(),
		})
	}
	return objects, nil
}

// RemoveCompacting deletes a claimed hot segment after checkpoint persistence.
func (store *LocalTierStore) RemoveCompacting(_ context.Context, obj TierObject) error {
	if obj.Path == "" {
		return fmt.Errorf("empty compacting path")
	}
	return os.Remove(obj.Path)
}

package logcompactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryObjectStore is an in-process S3 substitute for chaos and unit tests.
type MemoryObjectStore struct {
	mu      sync.RWMutex
	objects map[string]memoryObject
}

type memoryObject struct {
	data     []byte
	modTime  time.Time
	metadata map[string]string
}

// NewMemoryObjectStore returns an empty in-memory object store.
func NewMemoryObjectStore() *MemoryObjectStore {
	return &MemoryObjectStore{objects: make(map[string]memoryObject)}
}

// Put stores bytes under key.
func (store *MemoryObjectStore) Put(key string, data []byte, modTime time.Time, metadata map[string]string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	metaCopy := make(map[string]string, len(metadata))
	for k, v := range metadata {
		metaCopy[k] = v
	}
	store.objects[key] = memoryObject{
		data:     append([]byte(nil), data...),
		modTime:  modTime,
		metadata: metaCopy,
	}
}

// Get returns object bytes.
func (store *MemoryObjectStore) Get(key string) ([]byte, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	object, ok := store.objects[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), object.data...), true
}

// Delete removes key.
func (store *MemoryObjectStore) Delete(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.objects, key)
}

// List returns keys with prefix.
func (store *MemoryObjectStore) List(prefix string) []memoryListedObject {
	store.mu.RLock()
	defer store.mu.RUnlock()
	var objects []memoryListedObject
	for key, object := range store.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		objects = append(objects, memoryListedObject{
			key:     key,
			modTime: object.modTime,
			size:    int64(len(object.data)),
		})
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].key < objects[j].key
	})
	return objects
}

type memoryListedObject struct {
	key     string
	modTime time.Time
	size    int64
}

// MemoryS3TierStore implements tier compaction against MemoryObjectStore.
type MemoryS3TierStore struct {
	hotPrefix  string
	warmPrefix string
	mem        *MemoryObjectStore
	local      *LocalTierStore
}

// NewMemoryS3TierStore returns a scratch-backed tier store backed by MemoryObjectStore.
func NewMemoryS3TierStore(scratchDir, hotPrefix, warmPrefix string, mem *MemoryObjectStore) *MemoryS3TierStore {
	if mem == nil {
		mem = NewMemoryObjectStore()
	}
	if hotPrefix != "" && !strings.HasSuffix(hotPrefix, "/") {
		hotPrefix += "/"
	}
	if warmPrefix != "" && !strings.HasSuffix(warmPrefix, "/") {
		warmPrefix += "/"
	}
	hotDir := filepath.Join(scratchDir, "hot")
	warmDir := filepath.Join(scratchDir, "warm")
	_ = os.MkdirAll(hotDir, 0o755)
	_ = os.MkdirAll(warmDir, 0o755)
	return &MemoryS3TierStore{
		hotPrefix:  hotPrefix,
		warmPrefix: warmPrefix,
		mem:        mem,
		local:      NewLocalTierStore(hotDir, warmDir),
	}
}

// ListHot syncs memory hot objects into scratch and lists them.
func (store *MemoryS3TierStore) ListHot(_ context.Context, olderThan time.Time) ([]TierObject, error) {
	for _, object := range store.mem.List(store.hotPrefix) {
		name := strings.TrimPrefix(object.key, store.hotPrefix)
		if name == "" || !isHotSegmentName(filepath.Base(name)) {
			continue
		}
		if !object.modTime.Before(olderThan) {
			continue
		}
		localPath := filepath.Join(store.local.SourceDir, filepath.Base(name))
		if _, err := os.Stat(localPath); err == nil {
			continue
		}
		data, ok := store.mem.Get(object.key)
		if !ok {
			continue
		}
		if err := os.WriteFile(localPath, data, 0o644); err != nil {
			return nil, err
		}
		if err := os.Chtimes(localPath, object.modTime, object.modTime); err != nil {
			return nil, err
		}
	}
	return store.local.ListHot(context.Background(), olderThan)
}

func (store *MemoryS3TierStore) uploadWarmArtifacts(destKey, sha256 string) error {
	warmPath := filepath.Join(store.local.WarmDir, destKey)
	data, err := os.ReadFile(warmPath)
	if err != nil {
		return err
	}
	store.mem.Put(store.warmPrefix+destKey, data, time.Now().UTC(), map[string]string{
		s3MetadataSHA256Key: sha256,
	})
	metaPath := strings.TrimSuffix(warmPath, ".zst") + ".meta.json"
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}
	metaDigest, err := computeFileDigest(metaPath)
	if err != nil {
		return err
	}
	metaKey := store.warmPrefix + strings.TrimSuffix(destKey, ".zst") + ".meta.json"
	store.mem.Put(metaKey, metaBytes, time.Now().UTC(), map[string]string{
		s3MetadataSHA256Key: metaDigest.SHA256,
	})
	return nil
}

// WriteWarm writes warm output locally and to memory store.
func (store *MemoryS3TierStore) WriteWarm(ctx context.Context, destKey string, plaintext []byte, meta CompactionMeta) error {
	if err := store.local.WriteWarm(ctx, destKey, plaintext, meta); err != nil {
		return err
	}
	return store.uploadWarmArtifacts(destKey, meta.DestSHA256)
}

// WriteWarmFromFile writes warm output locally and to memory store.
func (store *MemoryS3TierStore) WriteWarmFromFile(ctx context.Context, destKey, filteredPath string, meta CompactionMeta) (string, error) {
	destSHA, err := store.local.WriteWarmFromFile(ctx, destKey, filteredPath, meta)
	if err != nil {
		return "", err
	}
	if err := store.uploadWarmArtifacts(destKey, destSHA); err != nil {
		store.local.RemoveWarmArtifacts(destKey)
		return "", err
	}
	return destSHA, nil
}

// RemoveHot deletes scratch and memory hot objects.
func (store *MemoryS3TierStore) RemoveHot(ctx context.Context, obj TierObject) error {
	if err := store.local.RemoveHot(ctx, obj); err != nil {
		return err
	}
	store.mem.Delete(store.hotPrefix + hotKeyFromCompacting(obj.Key))
	return nil
}

// ClaimHot claims a scratch hot segment.
func (store *MemoryS3TierStore) ClaimHot(ctx context.Context, obj TierObject) (TierObject, error) {
	return store.local.ClaimHot(ctx, obj)
}

// RollbackHot restores a claimed scratch segment.
func (store *MemoryS3TierStore) RollbackHot(ctx context.Context, obj TierObject) error {
	return store.local.RollbackHot(ctx, obj)
}

// ListStuckCompacting returns claimed scratch segments.
func (store *MemoryS3TierStore) ListStuckCompacting(ctx context.Context) ([]TierObject, error) {
	return store.local.ListStuckCompacting(ctx)
}

// RemoveCompacting deletes claimed scratch and memory hot objects.
func (store *MemoryS3TierStore) RemoveCompacting(ctx context.Context, obj TierObject) error {
	hotKey := hotKeyFromCompacting(obj.Key)
	if err := store.local.RemoveCompacting(ctx, obj); err != nil {
		return err
	}
	store.mem.Delete(store.hotPrefix + hotKey)
	return nil
}

// RemoveWarmArtifacts deletes incomplete warm scratch artifacts.
func (store *MemoryS3TierStore) RemoveWarmArtifacts(destKey string) {
	store.local.RemoveWarmArtifacts(destKey)
}

// SeedHot inserts a hot object for tests.
func (store *MemoryS3TierStore) SeedHot(name string, data []byte, modTime time.Time) {
	store.mem.Put(store.hotPrefix+name, data, modTime, nil)
}

// WarmObjectCount returns warm-tier segment count (.compact.zst) in memory store.
func (store *MemoryS3TierStore) WarmObjectCount() int {
	count := 0
	for _, object := range store.mem.List(store.warmPrefix) {
		if strings.HasSuffix(object.key, ".compact.zst") {
			count++
		}
	}
	return count
}

// WarmObject returns warm object bytes by dest key.
func (store *MemoryS3TierStore) WarmObject(destKey string) ([]byte, bool) {
	return store.mem.Get(store.warmPrefix + destKey)
}

// HotObjectCount returns hot-tier object count in memory store.
func (store *MemoryS3TierStore) HotObjectCount() int {
	return len(store.mem.List(store.hotPrefix))
}

// String returns debug summary.
func (store *MemoryS3TierStore) String() string {
	return fmt.Sprintf("memory-s3 hot=%d warm=%d", store.HotObjectCount(), store.WarmObjectCount())
}

package logcompactor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Config configures the compactor scan loop and downsampling policy.
type Config struct {
	HotMinAge                time.Duration
	SampleRate               uint64
	DeleteSourceAfterCompact bool
	CheckpointPath           string
	WorkInterval             time.Duration
	WarmDir                  string
	SourceDir                string
}

// localTierOps extends TierStore with claim/verify lifecycle used by the local backend.
type localTierOps interface {
	TierStore
	ClaimHot(ctx context.Context, obj TierObject) (TierObject, error)
	RollbackHot(ctx context.Context, obj TierObject) error
	ListStuckCompacting(ctx context.Context) ([]TierObject, error)
	RemoveCompacting(ctx context.Context, obj TierObject) error
	WriteWarmFromFile(ctx context.Context, destKey, filteredPath string, meta CompactionMeta) (string, error)
	RemoveWarmArtifacts(destKey string)
}

// checkpointStore persists compaction progress across restarts.
type checkpointStore interface {
	Load() error
	IsCompacted(sourceKey, sourceSHA256 string) bool
	Get(sourceKey string) (CheckpointRecord, bool)
	Save(record CheckpointRecord) error
}

// Compactor scans hot-tier segments and writes downsampled warm-tier output.
type Compactor struct {
	cfg        Config
	store      TierStore
	local      localTierOps
	checkpoint checkpointStore
	decryptKey []byte
	leader     *FileLeaderLock
	mu         sync.Mutex
	inflight   map[string]struct{}
}

// CompactorOption configures optional compactor behaviour.
type CompactorOption func(*Compactor)

// WithLeaderLock enables single-writer leader election for multi-instance deployments.
func WithLeaderLock(lock *FileLeaderLock) CompactorOption {
	return func(c *Compactor) {
		c.leader = lock
	}
}

// NewCompactor wires tier storage and checkpoint persistence.
func NewCompactor(cfg Config, store TierStore, checkpoint checkpointStore, decryptKey []byte, opts ...CompactorOption) *Compactor {
	if cfg.SampleRate == 0 {
		cfg.SampleRate = 1000
	}
	if cfg.WorkInterval <= 0 {
		cfg.WorkInterval = time.Hour
	}
	local, _ := store.(localTierOps)
	if cfg.WarmDir == "" {
		if ls, ok := store.(*LocalTierStore); ok {
			cfg.WarmDir = ls.WarmDir
		}
	}
	if cfg.SourceDir == "" {
		if ls, ok := store.(*LocalTierStore); ok {
			cfg.SourceDir = ls.SourceDir
		}
	}
	c := &Compactor{
		cfg:        cfg,
		store:      store,
		local:      local,
		checkpoint: checkpoint,
		decryptKey: decryptKey,
		inflight:   make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run executes compaction passes until ctx is cancelled.
func (c *Compactor) Run(ctx context.Context) error {
	if err := c.checkpoint.Load(); err != nil {
		return err
	}

	if err := c.recoverStuckSegments(ctx); err != nil {
		return err
	}
	if err := c.runLeaderPass(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(c.cfg.WorkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.runLeaderPass(ctx); err != nil {
				return err
			}
		}
	}
}

func (c *Compactor) runLeaderPass(ctx context.Context) error {
	if c.leader != nil {
		acquired, err := c.leader.TryAcquire()
		if err != nil {
			return err
		}
		if !acquired {
			return nil
		}
		defer c.leader.Release()
	}
	if err := c.recoverStuckSegments(ctx); err != nil {
		return err
	}
	return c.RunOnce(ctx)
}

// RunOnce executes a single compaction scan pass.
func (c *Compactor) RunOnce(ctx context.Context) error {
	if c.local == nil {
		return ErrCloudStoreNotConfigured
	}

	cutoff := time.Now().Add(-c.cfg.HotMinAge)
	objects, err := c.store.ListHot(ctx, cutoff)
	if err != nil {
		segmentsListedErrors.Inc()
		return err
	}
	refreshHotLag(ctx, c.store, c.checkpoint)

	var failed int
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.compactHotObject(ctx, obj); err != nil {
			segmentsCompactErrors.Inc()
			failed++
			continue
		}
	}
	if failed > 0 {
		return ErrCompactionFailures
	}
	return nil
}

func (c *Compactor) recoverStuckSegments(ctx context.Context) error {
	if c.local == nil {
		return nil
	}
	stuck, err := c.local.ListStuckCompacting(ctx)
	if err != nil {
		return err
	}
	for _, obj := range stuck {
		hotKey := hotKeyFromCompacting(obj.Key)
		if record, ok := c.checkpoint.Get(hotKey); ok {
			destPath := filepath.Join(c.cfg.WarmDir, record.DestKey)
			if _, err := os.Stat(destPath); err == nil {
				_ = c.local.RemoveCompacting(ctx, obj)
				continue
			}
		}
		if err := c.compactClaimedObject(ctx, obj, hotKey); err != nil {
			segmentsCompactErrors.Inc()
		}
	}
	return nil
}

func (c *Compactor) compactHotObject(ctx context.Context, obj TierObject) error {
	digest, err := computeFileDigest(obj.Path)
	if err != nil {
		return err
	}
	if c.checkpoint.IsCompacted(obj.Key, digest.SHA256) {
		return nil
	}

	if !c.tryInflight(obj.Key) {
		return nil
	}
	defer c.releaseInflight(obj.Key)

	claimed, err := c.local.ClaimHot(ctx, obj)
	if err != nil {
		return err
	}
	return c.compactClaimedObject(ctx, claimed, obj.Key)
}

func (c *Compactor) compactClaimedObject(ctx context.Context, obj TierObject, sourceKey string) error {
	digest, err := computeFileDigest(obj.Path)
	if err != nil {
		_ = c.local.RollbackHot(ctx, obj)
		return err
	}
	if c.checkpoint.IsCompacted(sourceKey, digest.SHA256) {
		_ = c.local.RemoveCompacting(ctx, obj)
		return nil
	}

	destKey := warmDestKey(sourceKey)
	filteredPath := filepath.Join(c.cfg.WarmDir, destKey+filteredTmpExt)

	if err := os.MkdirAll(filepath.Dir(filteredPath), 0o755); err != nil {
		_ = c.local.RollbackHot(ctx, obj)
		return err
	}

	reader, err := openPlaintextSegment(obj.Path, c.decryptKey)
	if err != nil {
		_ = os.Remove(filteredPath)
		_ = c.local.RollbackHot(ctx, obj)
		return err
	}

	filteredFile, err := os.OpenFile(filteredPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		_ = reader.Close()
		_ = c.local.RollbackHot(ctx, obj)
		return err
	}

	stats, filterErr := filterSegmentStream(reader, c.cfg.SampleRate, filteredFile)
	closeErr := reader.Close()
	syncErr := filteredFile.Sync()
	fileCloseErr := filteredFile.Close()

	if filterErr != nil {
		_ = os.Remove(filteredPath)
		_ = c.local.RollbackHot(ctx, obj)
		return filterErr
	}
	if closeErr != nil {
		_ = os.Remove(filteredPath)
		_ = c.local.RollbackHot(ctx, obj)
		return closeErr
	}
	if syncErr != nil {
		_ = os.Remove(filteredPath)
		_ = c.local.RollbackHot(ctx, obj)
		return syncErr
	}
	if fileCloseErr != nil {
		_ = os.Remove(filteredPath)
		_ = c.local.RollbackHot(ctx, obj)
		return fileCloseErr
	}

	if err := verifyPlaintextSegment(filteredPath, stats.KeptCount); err != nil {
		_ = os.Remove(filteredPath)
		_ = c.local.RollbackHot(ctx, obj)
		return err
	}

	meta := CompactionMeta{
		SourceKey:     sourceKey,
		DestKey:       destKey,
		SourceSHA256:  digest.SHA256,
		OriginalCount: stats.OriginalCount,
		KeptCount:     stats.KeptCount,
		SampleRate:    c.cfg.SampleRate,
		CompactedAt:   time.Now().UTC(),
	}

	destSHA, err := c.local.WriteWarmFromFile(ctx, destKey, filteredPath, meta)
	if err != nil {
		c.local.RemoveWarmArtifacts(destKey)
		_ = os.Remove(filteredPath)
		_ = c.local.RollbackHot(ctx, obj)
		return err
	}
	_ = os.Remove(filteredPath)

	if err := verifyWarmSegment(filepath.Join(c.cfg.WarmDir, destKey), stats.KeptCount); err != nil {
		c.local.RemoveWarmArtifacts(destKey)
		_ = c.local.RollbackHot(ctx, obj)
		return err
	}

	record := CheckpointRecord{
		SourceKey:     sourceKey,
		DestKey:       destKey,
		SourceSHA256:  digest.SHA256,
		DestSHA256:    destSHA,
		OriginalCount: stats.OriginalCount,
		KeptCount:     stats.KeptCount,
		CompactedAt:   meta.CompactedAt,
	}
	if err := c.checkpoint.Save(record); err != nil {
		c.local.RemoveWarmArtifacts(destKey)
		_ = c.local.RollbackHot(ctx, obj)
		return err
	}

	if c.cfg.SourceDir != "" {
		if err := WriteCompactMarker(c.cfg.SourceDir, record); err != nil {
			c.local.RemoveWarmArtifacts(destKey)
			_ = c.local.RollbackHot(ctx, obj)
			return err
		}
	}

	segmentsCompactedTotal.Inc()
	if stats.OriginalCount > 0 {
		recordsKeptRatio.Observe(float64(stats.KeptCount) / float64(stats.OriginalCount))
		plaintextBytes, statErr := os.Stat(filepath.Join(c.cfg.WarmDir, destKey))
		if statErr == nil && obj.Size > 0 {
			compressionRatio.Observe(float64(plaintextBytes.Size()) / float64(obj.Size))
		}
	}

	if c.cfg.DeleteSourceAfterCompact {
		if err := c.local.RemoveCompacting(ctx, obj); err != nil && !os.IsNotExist(err) {
			return err
		}
		if c.cfg.SourceDir != "" {
			_ = RemoveCompactMarker(c.cfg.SourceDir, sourceKey)
		}
	}
	return nil
}

func verifyWarmSegment(path string, expectKept int64) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder, err := zstd.NewReader(file)
	if err != nil {
		return err
	}
	defer decoder.Close()

	got, err := countSegmentRecords(decoder)
	if err != nil {
		return err
	}
	if got != expectKept {
		return ErrVerifyRecordCount
	}
	return nil
}

func (c *Compactor) tryInflight(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.inflight[key]; exists {
		return false
	}
	c.inflight[key] = struct{}{}
	return true
}

func (c *Compactor) releaseInflight(key string) {
	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()
}

func warmDestKey(sourceKey string) string {
	base := strings.TrimSuffix(sourceKey, readySuffix)
	base = strings.TrimSuffix(base, ".log")
	return base + ".compact.zst"
}

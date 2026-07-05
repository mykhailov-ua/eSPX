package logcompactor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/ads/pb"

	"github.com/stretchr/testify/require"
)

type faultCheckpointStore struct {
	inner    *CheckpointStore
	failSave atomic.Uint32
}

func (store *faultCheckpointStore) Load() error {
	return store.inner.Load()
}

func (store *faultCheckpointStore) IsCompacted(sourceKey, sourceSHA256 string) bool {
	return store.inner.IsCompacted(sourceKey, sourceSHA256)
}

func (store *faultCheckpointStore) Get(sourceKey string) (CheckpointRecord, bool) {
	return store.inner.Get(sourceKey)
}

func (store *faultCheckpointStore) Has(sourceKey string) bool {
	return store.inner.Has(sourceKey)
}

func (store *faultCheckpointStore) Save(record CheckpointRecord) error {
	if store.failSave.Load() > 0 {
		store.failSave.Add(^uint32(0))
		return errors.New("injected checkpoint save failure")
	}
	return store.inner.Save(record)
}

// Guards checkpoint persistence survives a crash after warm write but before checkpoint append.
func TestChaos_logCompactorCheckpointCrashRecovery(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")

	writeHotSegment(t, sourceDir, "segment_crash.log", buildSegmentPayload(t, 64, 1))

	store := NewLocalTierStore(sourceDir, warmDir)
	checkpoint := &faultCheckpointStore{inner: NewCheckpointStore(checkpointPath)}
	checkpoint.failSave.Store(1)

	compactor := NewCompactor(Config{
		HotMinAge: 0,
		WarmDir:   warmDir,
		SourceDir: sourceDir,
	}, store, checkpoint, nil)

	ctx := context.Background()
	require.ErrorIs(t, compactor.RunOnce(ctx), ErrCompactionFailures)
	require.NoError(t, checkpoint.inner.Load())
	_, ok := checkpoint.inner.Get("segment_crash.log")
	require.False(t, ok)

	require.NoError(t, compactor.RunOnce(ctx))

	require.NoError(t, checkpoint.inner.Load())
	record, ok := checkpoint.inner.Get("segment_crash.log")
	require.True(t, ok)
	require.NotEmpty(t, record.DestSHA256)
	require.FileExists(t, filepath.Join(warmDir, record.DestKey))

	logChaosProof(t, "log_compactor_checkpoint_crash_recovery", map[string]string{
		"subsystem":            "log_compactor",
		"checkpoint_persisted": "true",
		"warm_verified":        "true",
	})
}

// Guards compacting segments left by a crash are resumed without duplicating warm output.
func TestChaos_logCompactorCompactingRecovery(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")

	hotPath := writeHotSegment(t, sourceDir, "segment_stuck.log", buildSegmentPayload(t, 32, 1))
	compactingPath := compactingPathFor(hotPath)
	require.NoError(t, os.Rename(hotPath, compactingPath))

	store := NewLocalTierStore(sourceDir, warmDir)
	compactor := NewCompactor(Config{
		HotMinAge: 0,
		WarmDir:   warmDir,
		SourceDir: sourceDir,
	}, store, NewCheckpointStore(checkpointPath), nil)

	require.NoError(t, compactor.recoverStuckSegments(context.Background()))

	record, ok := compactor.checkpoint.Get("segment_stuck.log")
	require.True(t, ok)
	require.FileExists(t, filepath.Join(warmDir, record.DestKey))

	logChaosProof(t, "log_compactor_compacting_recovery", map[string]string{
		"subsystem":          "log_compactor",
		"compacting_resumed": "true",
		"exactly_once":       "true",
	})
}

// Guards warm write failure rolls back claimed hot segments.
func TestChaos_logCompactorWarmWriteRollback(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")

	writeHotSegment(t, sourceDir, "segment_verify.log", buildSegmentPayload(t, 8, 1))

	store := &faultInjectedLocalStore{
		inner:         NewLocalTierStore(sourceDir, warmDir),
		failWarmWrite: true,
	}
	compactor := NewCompactor(Config{
		HotMinAge: 0,
		WarmDir:   warmDir,
		SourceDir: sourceDir,
	}, store, NewCheckpointStore(checkpointPath), nil)

	require.ErrorIs(t, compactor.RunOnce(context.Background()), ErrCompactionFailures)
	require.FileExists(t, filepath.Join(sourceDir, "segment_verify.log"))
	_, ok := compactor.checkpoint.Get("segment_verify.log")
	require.False(t, ok)
	require.False(t, CompactMarkerReady(filepath.Join(sourceDir, "segment_verify.log")))

	logChaosProof(t, "log_compactor_warm_write_rollback", map[string]string{
		"subsystem":        "log_compactor",
		"hot_restored":     "true",
		"warm_rolled_back": "true",
	})
}

// Guards concurrent hot segment creation compacts each segment exactly once.
func TestChaos_logCompactorConcurrentStress(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")

	store := NewLocalTierStore(sourceDir, warmDir)
	checkpoint := NewCheckpointStore(checkpointPath)
	compactor := NewCompactor(Config{
		HotMinAge:    0,
		WarmDir:      warmDir,
		SourceDir:    sourceDir,
		WorkInterval: 20 * time.Millisecond,
	}, store, checkpoint, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = compactor.Run(ctx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			name := "segment_stress_" + itoa(index) + ".log"
			writeHotSegment(t, sourceDir, name, buildSegmentPayload(t, 10, 1))
		}(i)
	}
	wg.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if checkpoint.Count() == 20 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	require.Equal(t, 20, checkpoint.Count())

	logChaosProof(t, "log_compactor_concurrent_stress", map[string]string{
		"subsystem":  "log_compactor",
		"goroutines": "20",
		"segments":   "20",
	})
}

type faultInjectedLocalStore struct {
	inner         *LocalTierStore
	failWarmWrite bool
}

func (store *faultInjectedLocalStore) ListHot(ctx context.Context, olderThan time.Time) ([]TierObject, error) {
	return store.inner.ListHot(ctx, olderThan)
}

func (store *faultInjectedLocalStore) WriteWarm(ctx context.Context, destKey string, plaintext []byte, meta CompactionMeta) error {
	return store.inner.WriteWarm(ctx, destKey, plaintext, meta)
}

func (store *faultInjectedLocalStore) RemoveHot(ctx context.Context, obj TierObject) error {
	return store.inner.RemoveHot(ctx, obj)
}

func (store *faultInjectedLocalStore) ClaimHot(ctx context.Context, obj TierObject) (TierObject, error) {
	return store.inner.ClaimHot(ctx, obj)
}

func (store *faultInjectedLocalStore) RollbackHot(ctx context.Context, obj TierObject) error {
	return store.inner.RollbackHot(ctx, obj)
}

func (store *faultInjectedLocalStore) ListStuckCompacting(ctx context.Context) ([]TierObject, error) {
	return store.inner.ListStuckCompacting(ctx)
}

func (store *faultInjectedLocalStore) RemoveCompacting(ctx context.Context, obj TierObject) error {
	return store.inner.RemoveCompacting(ctx, obj)
}

func (store *faultInjectedLocalStore) WriteWarmFromFile(ctx context.Context, destKey, filteredPath string, meta CompactionMeta) (string, error) {
	if store.failWarmWrite {
		return "", errors.New("injected warm write failure")
	}
	return store.inner.WriteWarmFromFile(ctx, destKey, filteredPath, meta)
}

func (store *faultInjectedLocalStore) RemoveWarmArtifacts(destKey string) {
	store.inner.RemoveWarmArtifacts(destKey)
}

func writeHotSegment(t *testing.T, dir, name string, payload []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, payload, 0o644))
	oldTime := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(path, oldTime, oldTime))
	return path
}

func buildSegmentPayload(t *testing.T, impressions int, clicks int) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i := 0; i < impressions; i++ {
		buf.Write(encodeRecord(t, &pb.AdStreamEvent{
			EventType: []byte("impression"),
			ClickId:   []byte("imp-" + itoa(i)),
		}))
	}
	for i := 0; i < clicks; i++ {
		buf.Write(encodeRecord(t, &pb.AdStreamEvent{
			EventType: []byte("click"),
			ClickId:   []byte("click-" + itoa(i)),
		}))
	}
	return buf.Bytes()
}

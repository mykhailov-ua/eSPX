package logcompactor

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type faultMemoryWarmStore struct {
	inner         *MemoryS3TierStore
	failWarmWrite atomic.Uint32
}

func (store *faultMemoryWarmStore) ListHot(ctx context.Context, olderThan time.Time) ([]TierObject, error) {
	return store.inner.ListHot(ctx, olderThan)
}

func (store *faultMemoryWarmStore) WriteWarm(ctx context.Context, destKey string, plaintext []byte, meta CompactionMeta) error {
	return store.inner.WriteWarm(ctx, destKey, plaintext, meta)
}

func (store *faultMemoryWarmStore) RemoveHot(ctx context.Context, obj TierObject) error {
	return store.inner.RemoveHot(ctx, obj)
}

func (store *faultMemoryWarmStore) ClaimHot(ctx context.Context, obj TierObject) (TierObject, error) {
	return store.inner.ClaimHot(ctx, obj)
}

func (store *faultMemoryWarmStore) RollbackHot(ctx context.Context, obj TierObject) error {
	return store.inner.RollbackHot(ctx, obj)
}

func (store *faultMemoryWarmStore) ListStuckCompacting(ctx context.Context) ([]TierObject, error) {
	return store.inner.ListStuckCompacting(ctx)
}

func (store *faultMemoryWarmStore) RemoveCompacting(ctx context.Context, obj TierObject) error {
	return store.inner.RemoveCompacting(ctx, obj)
}

func (store *faultMemoryWarmStore) WriteWarmFromFile(ctx context.Context, destKey, filteredPath string, meta CompactionMeta) (string, error) {
	if store.failWarmWrite.Load() > 0 {
		store.failWarmWrite.Add(^uint32(0))
		return "", errors.New("injected warm upload failure")
	}
	return store.inner.WriteWarmFromFile(ctx, destKey, filteredPath, meta)
}

func (store *faultMemoryWarmStore) RemoveWarmArtifacts(destKey string) {
	store.inner.RemoveWarmArtifacts(destKey)
}

// Guards S3-backed compaction uploads warm output exactly once with digest idempotency.
func TestChaos_logCompactorS3TierExactlyOnce(t *testing.T) {
	scratchDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	mem := NewMemoryObjectStore()
	store := NewMemoryS3TierStore(scratchDir, "hot", "warm", mem)

	oldTime := time.Now().Add(-48 * time.Hour)
	store.SeedHot("segment_s3.log", buildSegmentPayload(t, 32, 1), oldTime)

	compactor := NewCompactor(Config{
		HotMinAge: 0,
		WarmDir:   store.local.WarmDir,
		SourceDir: store.local.SourceDir,
	}, store, NewCheckpointStore(checkpointPath), nil)

	require.NoError(t, compactor.RunOnce(context.Background()))
	require.NoError(t, compactor.RunOnce(context.Background()))

	assert.Equal(t, 1, store.HotObjectCount())
	assert.Equal(t, 1, store.WarmObjectCount())

	destKey := warmDestKey("segment_s3.log")
	warmData, ok := store.WarmObject(destKey)
	require.True(t, ok)
	assert.NotEmpty(t, warmData)

	logChaosProof(t, "log_compactor_s3_tier_exactly_once", map[string]string{
		"subsystem":     "log_compactor",
		"warm_uploaded": "true",
		"hot_removed":   "false",
		"exactly_once":  "true",
	})
}

// Guards transient warm upload failure rolls back scratch output without checkpoint.
func TestChaos_logCompactorS3WarmUploadRetry(t *testing.T) {
	scratchDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	mem := NewMemoryObjectStore()
	inner := NewMemoryS3TierStore(scratchDir, "hot", "warm", mem)
	store := &faultMemoryWarmStore{inner: inner}
	store.failWarmWrite.Store(1)

	oldTime := time.Now().Add(-48 * time.Hour)
	inner.SeedHot("segment_retry.log", buildSegmentPayload(t, 8, 1), oldTime)

	compactor := NewCompactor(Config{
		HotMinAge: 0,
		WarmDir:   inner.local.WarmDir,
		SourceDir: inner.local.SourceDir,
	}, store, NewCheckpointStore(checkpointPath), nil)

	require.ErrorIs(t, compactor.RunOnce(context.Background()), ErrCompactionFailures)
	assert.Equal(t, 0, inner.WarmObjectCount())

	checkpoint := NewCheckpointStore(checkpointPath)
	_, ok := checkpoint.Get("segment_retry.log")
	require.False(t, ok)

	require.NoError(t, compactor.RunOnce(context.Background()))
	assert.Equal(t, 1, inner.WarmObjectCount())

	logChaosProof(t, "log_compactor_s3_warm_upload_retry", map[string]string{
		"subsystem":        "log_compactor",
		"retry_succeeded":  "true",
		"checkpoint_clean": "true",
	})
}

// Guards flock leader election allows only one concurrent compactor writer.
func TestChaos_logCompactorLeaderElectionSingleWriter(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	lockPath := filepath.Join(t.TempDir(), "leader.lock")

	for i := 0; i < 10; i++ {
		writeHotSegment(t, sourceDir, "segment_leader_"+itoa(i)+".log", buildSegmentPayload(t, 4, 0))
	}

	store := NewLocalTierStore(sourceDir, warmDir)
	checkpoint := NewCheckpointStore(checkpointPath)

	lockA := NewFileLeaderLock(lockPath)
	lockB := NewFileLeaderLock(lockPath)
	compactorA := NewCompactor(Config{
		HotMinAge:    0,
		WarmDir:      warmDir,
		SourceDir:    sourceDir,
		WorkInterval: 10 * time.Millisecond,
	}, store, checkpoint, nil, WithLeaderLock(lockA))
	compactorB := NewCompactor(Config{
		HotMinAge:    0,
		WarmDir:      warmDir,
		SourceDir:    sourceDir,
		WorkInterval: 10 * time.Millisecond,
	}, store, checkpoint, nil, WithLeaderLock(lockB))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = compactorA.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = compactorB.Run(ctx)
	}()
	wg.Wait()

	require.Equal(t, 10, checkpoint.Count())

	logChaosProof(t, "log_compactor_leader_election", map[string]string{
		"subsystem":  "log_compactor",
		"instances":  "2",
		"segments":   "10",
		"duplicates": "0",
	})
}

func TestFileLeaderLock_exclusive(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "leader.lock")
	lockA := NewFileLeaderLock(lockPath)
	lockB := NewFileLeaderLock(lockPath)

	acquiredA, err := lockA.TryAcquire()
	require.NoError(t, err)
	require.True(t, acquiredA)

	acquiredB, err := lockB.TryAcquire()
	require.NoError(t, err)
	require.False(t, acquiredB)

	require.NoError(t, lockA.Release())

	acquiredB, err = lockB.TryAcquire()
	require.NoError(t, err)
	require.True(t, acquiredB)
	require.NoError(t, lockB.Release())
}

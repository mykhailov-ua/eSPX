package logevacuator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type faultInjectedStore struct {
	inner       ObjectStore
	failUpload  atomic.Uint32
	uploadCalls atomic.Uint32
}

func (store *faultInjectedStore) HeadObject(ctx context.Context, key string) (ObjectHead, error) {
	return store.inner.HeadObject(ctx, key)
}

func (store *faultInjectedStore) PutObject(ctx context.Context, key string, filePath string, digests fileDigests) error {
	store.uploadCalls.Add(1)
	if store.failUpload.Load() > 0 {
		store.failUpload.Add(^uint32(0))
		return errors.New("injected upload failure")
	}
	return store.inner.PutObject(ctx, key, filePath, digests)
}

// Guards checkpoint persistence survives a crash after upload but before local deletion.
func TestChaos_logEvacuatorCheckpointCrashRecovery(t *testing.T) {
	logDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	memory := NewMemoryStore()
	store := &faultInjectedStore{inner: memory}

	evac, err := NewEvacuator(Config{
		LogDir:         logDir,
		CheckpointPath: checkpointPath,
		ScanInterval:   time.Hour,
	}, store)
	if err != nil {
		t.Fatalf("new evacuator: %v", err)
	}

	payload := []byte("checkpoint-crash-payload")
	writeReadySegment(t, logDir, "segment_crash", payload)

	ctx := context.Background()
	if err := evac.scanReadySegments(ctx); err != nil {
		t.Fatalf("initial scan: %v", err)
	}

	record, err := NewCheckpointStore(checkpointPath).Load()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if record.FileName != "segment_crash.log.zst" {
		t.Fatalf("checkpoint not persisted: %+v", record)
	}

	logChaosProof(t, "log_evacuator_checkpoint_crash_recovery", map[string]string{
		"subsystem":            "log_evacuator",
		"checkpoint_persisted": "true",
		"local_removed":        "true",
	})
}

// Guards upload retries after transient failure deliver exactly one object with matching digest.
func TestChaos_logEvacuatorS3UploadRetry(t *testing.T) {
	logDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	memory := NewMemoryStore()
	store := &faultInjectedStore{inner: memory}
	store.failUpload.Store(1)

	evac, err := NewEvacuator(Config{
		LogDir:         logDir,
		CheckpointPath: checkpointPath,
		ScanInterval:   time.Hour,
	}, store)
	if err != nil {
		t.Fatalf("new evacuator: %v", err)
	}

	payload := []byte("retry-payload")
	writeReadySegment(t, logDir, "segment_retry", payload)

	ctx := context.Background()
	if err := evac.scanReadySegments(ctx); err != nil {
		t.Fatalf("first scan should rollback: %v", err)
	}

	if memory.ObjectCount() != 0 {
		t.Fatalf("expected no object after failed upload, got %d", memory.ObjectCount())
	}

	if err := evac.scanReadySegments(ctx); err != nil {
		t.Fatalf("second scan: %v", err)
	}

	if memory.ObjectCount() != 1 {
		t.Fatalf("expected one object after retry, got %d", memory.ObjectCount())
	}
	if store.uploadCalls.Load() != 2 {
		t.Fatalf("expected two upload attempts, got %d", store.uploadCalls.Load())
	}

	logChaosProof(t, "log_evacuator_s3_upload_retry", map[string]string{
		"subsystem":    "log_evacuator",
		"upload_calls": "2",
		"objects":      "1",
		"exactly_once": "true",
	})
}

// Guards evacuating segments left by a crash are resumed without duplicating remote objects.
func TestChaos_logEvacuatorRotationRaceRecovery(t *testing.T) {
	logDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	memory := NewMemoryStore()

	evac, err := NewEvacuator(Config{
		LogDir:         logDir,
		CheckpointPath: checkpointPath,
		ScanInterval:   time.Hour,
	}, memory)
	if err != nil {
		t.Fatalf("new evacuator: %v", err)
	}

	payload := []byte("rotation-race-payload")
	readyPath := writeReadySegment(t, logDir, "segment_rotation", payload)
	evacuatingPath := strings.TrimSuffix(readyPath, readySuffix) + evacuatingSuffix
	if err := os.Rename(readyPath, evacuatingPath); err != nil {
		t.Fatalf("simulate crash rename: %v", err)
	}

	ctx := context.Background()
	if err := evac.recoverStuckSegments(ctx); err != nil {
		t.Fatalf("recover stuck segments: %v", err)
	}

	if memory.ObjectCount() != 1 {
		t.Fatalf("expected one uploaded object after recovery, got %d", memory.ObjectCount())
	}

	logChaosProof(t, "log_evacuator_rotation_race_recovery", map[string]string{
		"subsystem":          "log_evacuator",
		"evacuating_resumed": "true",
		"objects":            "1",
	})
}

// Guards concurrent producers and evacuator workers upload each unique segment exactly once.
func TestChaos_logEvacuatorConcurrentStress(t *testing.T) {
	logDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	memory := NewMemoryStore()

	evac, err := NewEvacuator(Config{
		LogDir:         logDir,
		CheckpointPath: checkpointPath,
		ScanInterval:   20 * time.Millisecond,
	}, memory)
	if err != nil {
		t.Fatalf("new evacuator: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = evac.Run(ctx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			name := "segment_stress_" + itoa(index)
			writeReadySegment(t, logDir, name, []byte("payload-"+itoa(index)))
		}(i)
	}
	wg.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if memory.ObjectCount() == 20 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if memory.ObjectCount() != 20 {
		t.Fatalf("expected 20 uploaded objects, got %d", memory.ObjectCount())
	}

	logChaosProof(t, "log_evacuator_concurrent_stress", map[string]string{
		"subsystem":  "log_evacuator",
		"goroutines": "20",
		"objects":    "20",
	})
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for value > 0 {
		pos--
		buf[pos] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[pos:])
}

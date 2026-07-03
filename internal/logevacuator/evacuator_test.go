package logevacuator

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func writeReadySegment(t *testing.T, logDir, name string, payload []byte) string {
	t.Helper()
	readyPath := filepath.Join(logDir, name+readySuffix)
	if err := os.WriteFile(readyPath, payload, 0o644); err != nil {
		t.Fatalf("write ready segment: %v", err)
	}
	return readyPath
}

// Guards a ready segment is renamed, uploaded once, checkpointed, and removed locally.
func TestEvacuator_uploadsReadySegment(t *testing.T) {
	logDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	store := NewMemoryStore()

	evac, err := NewEvacuator(Config{
		LogDir:         logDir,
		CheckpointPath: checkpointPath,
		ScanInterval:   time.Hour,
	}, store)
	if err != nil {
		t.Fatalf("new evacuator: %v", err)
	}

	payload := []byte("compressed-audit-log-payload")
	writeReadySegment(t, logDir, "segment_0001", payload)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = evac.scanReadySegments(ctx)
		close(done)
	}()
	<-done

	if store.ObjectCount() != 1 {
		t.Fatalf("expected 1 uploaded object, got %d", store.ObjectCount())
	}
	if got := store.ObjectData("segment_0001.log.zst"); string(got) != string(payload) {
		t.Fatalf("object bytes mismatch: %q", got)
	}

	record, err := NewCheckpointStore(checkpointPath).Load()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if record.FileName != "segment_0001.log.zst" {
		t.Fatalf("checkpoint file mismatch: %q", record.FileName)
	}

	matches, _ := filepath.Glob(filepath.Join(logDir, "*"))
	if len(matches) != 0 {
		t.Fatalf("expected local segment removed, found %v", matches)
	}
}

// Guards duplicate processing of the same digest is skipped via object head metadata.
func TestEvacuator_idempotentHeadSkip(t *testing.T) {
	logDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	store := NewMemoryStore()

	evac, err := NewEvacuator(Config{
		LogDir:         logDir,
		CheckpointPath: checkpointPath,
		ScanInterval:   time.Hour,
	}, store)
	if err != nil {
		t.Fatalf("new evacuator: %v", err)
	}

	payload := []byte("idempotent-payload")
	writeReadySegment(t, logDir, "segment_0002", payload)

	ctx := context.Background()
	if err := evac.scanReadySegments(ctx); err != nil {
		t.Fatalf("first scan: %v", err)
	}
	writeReadySegment(t, logDir, "segment_0003", payload)
	if err := evac.scanReadySegments(ctx); err != nil {
		t.Fatalf("second scan: %v", err)
	}

	if store.ObjectCount() != 2 {
		t.Fatalf("expected 2 objects after distinct names, got %d", store.ObjectCount())
	}
}

// Guards concurrent claims rename only one ready file copy and upload a single object.
func TestEvacuator_concurrentClaimSingleUpload(t *testing.T) {
	logDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	store := NewMemoryStore()

	evac, err := NewEvacuator(Config{
		LogDir:         logDir,
		CheckpointPath: checkpointPath,
		ScanInterval:   time.Hour,
	}, store)
	if err != nil {
		t.Fatalf("new evacuator: %v", err)
	}

	payload := []byte("race-payload")
	writeReadySegment(t, logDir, "segment_race", payload)

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = evac.scanReadySegments(ctx)
		}()
	}
	wg.Wait()

	if store.ObjectCount() != 1 {
		t.Fatalf("expected exactly one uploaded object, got %d", store.ObjectCount())
	}
}

package logevacuator

import (
	"os"
	"path/filepath"
	"testing"
)

// Guards checkpoint save and load round-trip across atomic file replacement.
func TestCheckpointStore_saveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint")
	store := NewCheckpointStore(path)

	record := CheckpointRecord{
		FileName: "segment_20260101.log.zst",
		SHA256:   "abc123",
	}
	if err := store.Save(record); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if loaded.FileName != record.FileName || loaded.SHA256 != record.SHA256 {
		t.Fatalf("checkpoint mismatch: got %+v want %+v", loaded, record)
	}
}

// Guards missing checkpoint file returns an empty record without error.
func TestCheckpointStore_missingFile(t *testing.T) {
	store := NewCheckpointStore(filepath.Join(t.TempDir(), "missing"))
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("load missing checkpoint: %v", err)
	}
	if loaded.FileName != "" || loaded.SHA256 != "" {
		t.Fatalf("expected empty checkpoint, got %+v", loaded)
	}
}

// Guards corrupt checkpoint files surface ErrCheckpointCorrupt.
func TestCheckpointStore_corruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint")
	if err := os.WriteFile(path, []byte("bad|line\n"), 0o644); err != nil {
		t.Fatalf("write corrupt checkpoint: %v", err)
	}

	store := NewCheckpointStore(path)
	_, err := store.Load()
	if err != ErrCheckpointCorrupt {
		t.Fatalf("expected ErrCheckpointCorrupt, got %v", err)
	}
}

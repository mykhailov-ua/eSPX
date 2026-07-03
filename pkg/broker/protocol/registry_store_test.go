package protocol

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileRegistryStoreRoundtrip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileRegistryStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	snap := RegistrySnapshot{
		Version: registryFileVersion,
		Topics: map[string]uint16{
			"tracker-logs": 1,
			"fraud-stream": 2,
		},
		NextID: 3,
	}
	if err := store.Save(snap); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Topics["tracker-logs"] != 1 {
		t.Fatalf("tracker-logs id: got %d want 1", loaded.Topics["tracker-logs"])
	}
	if loaded.NextID != 3 {
		t.Fatalf("next_id: got %d want 3", loaded.NextID)
	}
	if _, err := os.Stat(filepath.Join(dir, ".topics", "registry.json")); err != nil {
		t.Fatalf("registry file missing: %v", err)
	}
}

func TestTopicRegistryPersistsAcrossReload(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileRegistryStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	r1 := NewTopicRegistry()
	r1.SetFileStore(store)

	id1, err := r1.Register("topic-a")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := r1.Register("topic-b")
	if err != nil {
		t.Fatal(err)
	}

	r2 := NewTopicRegistry()
	r2.SetFileStore(store)
	if err := r2.Load(); err != nil {
		t.Fatal(err)
	}

	got1, err := r2.Register("topic-a")
	if err != nil {
		t.Fatal(err)
	}
	got2, err := r2.Register("topic-b")
	if err != nil {
		t.Fatal(err)
	}
	if got1 != id1 || got2 != id2 {
		t.Fatalf("ids changed after reload: got (%d,%d) want (%d,%d)", got1, got2, id1, id2)
	}

	id3, err := r2.Register("topic-c")
	if err != nil {
		t.Fatal(err)
	}
	if id3 <= id2 {
		t.Fatalf("expected monotonic id, got %d after %d", id3, id2)
	}
}

func TestTopicRegistryMergeClusterWins(t *testing.T) {
	r := NewTopicRegistry()
	if _, err := r.Register("local-topic"); err != nil {
		t.Fatal(err)
	}

	err := r.Merge(RegistrySnapshot{
		Topics: map[string]uint16{
			"cluster-topic": 42,
		},
		NextID: 43,
	})
	if err != nil {
		t.Fatal(err)
	}

	meta, ok := r.Lookup(42)
	if !ok || meta.Name != "cluster-topic" {
		t.Fatalf("cluster topic not installed: %+v ok=%v", meta, ok)
	}
}

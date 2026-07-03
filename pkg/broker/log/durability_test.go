package log

import (
	"os"
	"testing"
	"time"
)

func TestParseDurabilityMode(t *testing.T) {
	tests := []struct {
		in   string
		want DurabilityMode
	}{
		{"", DurabilityAsync},
		{"async", DurabilityAsync},
		{"group", DurabilityGroupCommit},
		{"group_commit", DurabilityGroupCommit},
		{"sync", DurabilitySync},
	}
	for _, tc := range tests {
		got, err := ParseDurabilityMode(tc.in)
		if err != nil {
			t.Fatalf("ParseDurabilityMode(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseDurabilityMode(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if _, err := ParseDurabilityMode("invalid"); err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

func TestDurability_GroupCommitThreshold(t *testing.T) {
	dir, err := os.MkdirTemp("", "durability-group-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := DurabilityConfig{
		Mode:               DurabilityGroupCommit,
		FlushInterval:      time.Hour,
		GroupCommitRecords: 2,
	}
	pl, err := NewPartitionLogWithDurability(dir, 1024*1024, 4096, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if _, err := pl.Append([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if pl.PendingFsync() != 1 {
		t.Fatalf("expected pending 1, got %d", pl.PendingFsync())
	}
	if _, err := pl.Append([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if pl.PendingFsync() != 0 {
		t.Fatalf("expected pending reset to 0 after threshold, got %d", pl.PendingFsync())
	}
}

func TestDurability_SyncModeClearsPending(t *testing.T) {
	dir, err := os.MkdirTemp("", "durability-sync-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := DurabilityConfig{
		Mode:          DurabilitySync,
		FlushInterval: time.Hour,
	}
	pl, err := NewPartitionLogWithDurability(dir, 1024*1024, 4096, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if _, err := pl.Append([]byte("synced")); err != nil {
		t.Fatal(err)
	}
	if pl.NextOffset() != 1 {
		t.Fatalf("expected offset 1, got %d", pl.NextOffset())
	}
}

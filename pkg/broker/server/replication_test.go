package server

import (
	"os"
	"testing"

	"espx/pkg/broker/log"
)

// TestChaos_Replication_GapDetection proves a skipped leader offset does not corrupt the follower log.
func TestChaos_Replication_GapDetection(t *testing.T) {
	dir, err := os.MkdirTemp("", "replication-gap-chaos-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pl, err := log.NewPartitionLog(dir, 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if _, err := pl.AppendReplicatedAt(0, []byte("leader-0")); err != nil {
		t.Fatal(err)
	}
	if _, err := pl.AppendReplicatedAt(1, []byte("leader-1")); err != nil {
		t.Fatal(err)
	}

	before := pl.NextOffset()
	if _, err := pl.AppendReplicatedAt(3, []byte("skipped-2")); err != log.ErrReplicationGap {
		t.Fatalf("expected replication gap, got %v", err)
	}
	if pl.NextOffset() != before {
		t.Fatalf("follower advanced on gap: before=%d after=%d", before, pl.NextOffset())
	}

	if _, err := pl.AppendReplicatedAt(2, []byte("leader-2")); err != nil {
		t.Fatal(err)
	}
	if pl.NextOffset() != 3 {
		t.Fatalf("expected nextOffset 3 after gap heal, got %d", pl.NextOffset())
	}

	t.Logf("chaos_proof fault=replication_gap expected=3 got_halt=true next_offset_preserved=%d healed=true", before)
}

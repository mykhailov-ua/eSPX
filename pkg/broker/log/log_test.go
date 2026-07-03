package log

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestSegmentWriteAndRead smoke-tests mmap append and offset lookup.
func TestSegmentWriteAndRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	seg, err := NewSegment(dir, 0, 1024*1024, 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	payload := []byte("hello world")
	pos, err := seg.Write(100, payload)
	if err != nil {
		t.Fatal(err)
	}

	if pos != 0 {
		t.Errorf("expected pos 0, got %d", pos)
	}

	targetPos, msgCount, totalMsgBytes, err := seg.LocateMessages(0, 100, 1024)
	if err != nil {
		t.Fatal(err)
	}

	if targetPos != 0 {
		t.Errorf("expected targetPos 0, got %d", targetPos)
	}
	if msgCount != 1 {
		t.Errorf("expected msgCount 1, got %d", msgCount)
	}
	expectedLen := uint32(12 + len(payload))
	if totalMsgBytes != expectedLen {
		t.Errorf("expected totalMsgBytes %d, got %d", expectedLen, totalMsgBytes)
	}
}

// BenchmarkSegmentWrite tracks append throughput regression on partition logs.
func BenchmarkSegmentWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "segment-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	maxSize := int64(b.N) * 100
	if maxSize < 1024*1024 {
		maxSize = 1024 * 1024
	}
	seg, err := NewSegment(dir, 0, maxSize, 4096, true)
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	payload := []byte("hello world")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := seg.Write(uint64(i), payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestSegmentRecoveryWithMalformedTail ensures recovery truncates torn tail records safely.
func TestSegmentRecoveryWithMalformedTail(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-recovery-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	seg, err := NewSegment(dir, 0, 1024*1024, 4096, true)
	if err != nil {
		t.Fatal(err)
	}

	p1 := []byte("first valid record")
	pos1, err := seg.Write(100, p1)
	if err != nil {
		t.Fatal(err)
	}

	p2 := []byte("second valid record")
	pos2, err := seg.Write(101, p2)
	if err != nil {
		t.Fatal(err)
	}

	err = seg.Close()
	if err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", 0))
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write([]byte{0, 0, 0, 100})
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	seg2, err := NewSegment(dir, 0, 1024*1024, 4096, true)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()

	nextOffset, err := seg2.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	t.Logf("After recovery: nextOffset=%d, logSize=%d, maxSegSize=%d", nextOffset, seg2.logSize, seg2.maxSegSize)

	p3 := []byte("third valid record")
	pos3, err := seg2.Write(nextOffset, p3)
	if err != nil {
		t.Fatalf("Write failed: %v (logSize=%d, maxSegSize=%d)", err, seg2.logSize, seg2.maxSegSize)
	}

	if pos1 != 0 {
		t.Errorf("expected pos1 to be 0, got %d", pos1)
	}
	expectedPos2 := int64(12 + len(p1))
	if pos2 != expectedPos2 {
		t.Errorf("expected pos2 to be %d, got %d", expectedPos2, pos2)
	}
	expectedPos3 := expectedPos2 + int64(12+len(p2))
	if pos3 != expectedPos3 {
		t.Errorf("expected pos3 to be %d, got %d", expectedPos3, pos3)
	}

	targetPos, msgCount, totalMsgBytes, err := seg2.LocateMessages(0, 100, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	if targetPos != 0 {
		t.Errorf("expected targetPos to be 0, got %d", targetPos)
	}
	if msgCount != 3 {
		t.Errorf("expected msgCount to be 3, got %d", msgCount)
	}
	expectedTotalBytes := uint32(12 + len(p1) + 12 + len(p2) + 12 + len(p3))
	if totalMsgBytes != expectedTotalBytes {
		t.Errorf("expected totalMsgBytes to be %d, got %d", expectedTotalBytes, totalMsgBytes)
	}
}

// TestFencingEpoch_RejectsStaleToken ensures the storage layer blocks demoted leader writes.
func TestFencingEpoch_RejectsStaleToken(t *testing.T) {
	dir, err := os.MkdirTemp("", "fencing-epoch-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pl, err := NewPartitionLog(dir, 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if _, err := pl.AppendFenced(3, []byte("term-3")); err != nil {
		t.Fatal(err)
	}
	if pl.FencingEpoch() != 3 {
		t.Fatalf("expected fencing epoch 3, got %d", pl.FencingEpoch())
	}

	if _, err := pl.AppendFenced(2, []byte("stale")); err != ErrStaleFencingEpoch {
		t.Fatalf("expected ErrStaleFencingEpoch, got %v", err)
	}

	if _, err := pl.AppendFenced(4, []byte("term-4")); err != nil {
		t.Fatal(err)
	}
}

// TestFencingEpoch_PersistedAcrossRestart reloads the fencing floor after broker restart.
func TestFencingEpoch_PersistedAcrossRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "fencing-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pl, err := NewPartitionLog(dir, 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pl.AppendFenced(7, []byte("epoch-7")); err != nil {
		t.Fatal(err)
	}
	if err := pl.Close(); err != nil {
		t.Fatal(err)
	}

	pl2, err := NewPartitionLog(dir, 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer pl2.Close()

	if pl2.FencingEpoch() != 7 {
		t.Fatalf("expected reloaded epoch 7, got %d", pl2.FencingEpoch())
	}
	if _, err := pl2.AppendFenced(6, []byte("stale-after-restart")); err != ErrStaleFencingEpoch {
		t.Fatalf("expected stale epoch after restart, got %v", err)
	}
}

// TestFencingEpoch_AdvanceBlocksStaleLeader simulates demotion by raising the cluster epoch floor.
func TestFencingEpoch_AdvanceBlocksStaleLeader(t *testing.T) {
	dir, err := os.MkdirTemp("", "fencing-advance-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pl, err := NewPartitionLog(dir, 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if _, err := pl.AppendFenced(2, []byte("leader-term-2")); err != nil {
		t.Fatal(err)
	}
	if err := pl.AdvanceFencingEpoch(5); err != nil {
		t.Fatal(err)
	}
	if _, err := pl.AppendFenced(2, []byte("stale-leader")); err != ErrStaleFencingEpoch {
		t.Fatalf("expected stale leader rejection, got %v", err)
	}
	if _, err := pl.AppendFenced(5, []byte("new-leader")); err != nil {
		t.Fatal(err)
	}
}

// TestReplication_OrderedAppend requires follower entries to use the leader offset sequence.
func TestReplication_OrderedAppend(t *testing.T) {
	dir, err := os.MkdirTemp("", "replication-ordered-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pl, err := NewPartitionLog(dir, 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if off, err := pl.AppendReplicatedAt(0, []byte("m0")); err != nil || off != 0 {
		t.Fatalf("offset 0: off=%d err=%v", off, err)
	}
	if off, err := pl.AppendReplicatedAt(1, []byte("m1")); err != nil || off != 1 {
		t.Fatalf("offset 1: off=%d err=%v", off, err)
	}
	if pl.NextOffset() != 2 {
		t.Fatalf("expected nextOffset 2, got %d", pl.NextOffset())
	}
}

// TestReplication_GapDetection halts when the leader log skips an expected offset.
func TestReplication_GapDetection(t *testing.T) {
	dir, err := os.MkdirTemp("", "replication-gap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pl, err := NewPartitionLog(dir, 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if _, err := pl.AppendReplicatedAt(0, []byte("m0")); err != nil {
		t.Fatal(err)
	}
	if _, err := pl.AppendReplicatedAt(2, []byte("gap")); err != ErrReplicationGap {
		t.Fatalf("expected ErrReplicationGap, got %v", err)
	}
	if pl.NextOffset() != 1 {
		t.Fatalf("nextOffset must not advance on gap, got %d", pl.NextOffset())
	}
}

// TestReplication_DuplicateSkipped treats already-applied offsets as idempotent no-ops.
func TestReplication_DuplicateSkipped(t *testing.T) {
	dir, err := os.MkdirTemp("", "replication-dup-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	pl, err := NewPartitionLog(dir, 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Close()

	if _, err := pl.AppendReplicatedAt(0, []byte("m0")); err != nil {
		t.Fatal(err)
	}
	if _, err := pl.AppendReplicatedAt(0, []byte("dup")); err != nil {
		t.Fatalf("duplicate offset should be skipped, got %v", err)
	}
	if pl.NextOffset() != 1 {
		t.Fatalf("expected nextOffset 1 after duplicate skip, got %d", pl.NextOffset())
	}
	if _, err := pl.AppendReplicatedAt(1, []byte("m1")); err != nil {
		t.Fatal(err)
	}
}

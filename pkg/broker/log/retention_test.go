package log

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTestPartition(t *testing.T, maxSegSize int64) (*PartitionLog, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "retention-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	pl, err := NewPartitionLog(dir, maxSegSize, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pl.Close() })
	return pl, dir
}

func appendMessages(t *testing.T, pl *PartitionLog, n int, payloadSize int) {
	t.Helper()
	payload := make([]byte, payloadSize)
	for i := 0; i < n; i++ {
		if _, err := pl.Append(payload); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

func countSegmentFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			n++
		}
	}
	return n
}

func TestRetention_DisabledIsNoOp(t *testing.T) {
	pl, dir := openTestPartition(t, 256)
	appendMessages(t, pl, 4, 32)

	result, err := pl.ApplyRetention(RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 0 {
		t.Fatalf("expected 0 deletes, got %d", result.DeletedSegments)
	}
	if countSegmentFiles(t, dir) != 1 {
		t.Fatalf("expected 1 segment, got %d", countSegmentFiles(t, dir))
	}
}

func TestRetention_AgeDeletesOldSealedSegments(t *testing.T) {
	pl, dir := openTestPartition(t, 256)
	appendMessages(t, pl, 8, 32)

	snap := pl.snap.Load()
	if len(snap.segments) < 2 {
		t.Fatalf("expected rolled segments, got %d", len(snap.segments))
	}

	oldSeg := snap.segments[0]
	oldTime := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldSeg.logPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	policy := RetentionPolicy{
		MaxAge:         24 * time.Hour,
		SafetyMessages: 0,
	}
	result, err := pl.ApplyRetention(policy)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 1 {
		t.Fatalf("expected 1 deleted segment, got %d", result.DeletedSegments)
	}
	if countSegmentFiles(t, dir) != len(snap.segments)-1 {
		t.Fatalf("unexpected segment file count: %d", countSegmentFiles(t, dir))
	}

	_, _, err = pl.ReadRawMessages(oldSeg.baseOffset, 64*1024)
	if err == nil {
		t.Fatal("expected fetch from deleted segment to fail")
	}
}

func TestRetention_BytesCapEvictsOldest(t *testing.T) {
	pl, dir := openTestPartition(t, 256)
	appendMessages(t, pl, 12, 48)

	before := countSegmentFiles(t, dir)
	if before < 2 {
		t.Fatalf("need multiple segments, got %d", before)
	}

	policy := RetentionPolicy{
		MaxBytes:       400,
		SafetyMessages: 0,
	}
	result, err := pl.ApplyRetention(policy)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments == 0 {
		t.Fatal("expected byte cap to delete at least one sealed segment")
	}
	if countSegmentFiles(t, dir) >= before {
		t.Fatalf("expected fewer segments, before=%d after=%d", before, countSegmentFiles(t, dir))
	}
}

func TestRetention_NeverDeletesActive(t *testing.T) {
	pl, dir := openTestPartition(t, 1024)
	appendMessages(t, pl, 2, 16)

	active := pl.snap.Load().activeSeg
	oldTime := time.Now().Add(-72 * time.Hour)
	if err := os.Chtimes(active.logPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	policy := RetentionPolicy{
		MaxAge:         time.Hour,
		SafetyMessages: 0,
	}
	result, err := pl.ApplyRetention(policy)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 0 {
		t.Fatalf("active segment must survive, deleted=%d", result.DeletedSegments)
	}
	if countSegmentFiles(t, dir) != 1 {
		t.Fatalf("active segment file missing")
	}
}

func TestRetention_SafetyMessagesPreservesTail(t *testing.T) {
	pl, dir := openTestPartition(t, 256)
	appendMessages(t, pl, 10, 32)

	snap := pl.snap.Load()
	if len(snap.segments) < 2 {
		t.Fatalf("expected multiple segments")
	}
	oldSeg := snap.segments[0]
	oldTime := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldSeg.logPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	policy := RetentionPolicy{
		MaxAge:         24 * time.Hour,
		SafetyMessages: 20,
	}
	result, err := pl.ApplyRetention(policy)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 0 {
		t.Fatalf("safety window should block deletion, deleted=%d", result.DeletedSegments)
	}
	if countSegmentFiles(t, dir) != len(snap.segments) {
		t.Fatalf("segment count changed unexpectedly")
	}
}

func TestRetention_FloorOffsetBlocksDeletion(t *testing.T) {
	pl, dir := openTestPartition(t, 256)
	appendMessages(t, pl, 8, 32)

	snap := pl.snap.Load()
	oldSeg := snap.segments[0]
	oldTime := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldSeg.logPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	policy := RetentionPolicy{
		MaxAge:         24 * time.Hour,
		SafetyMessages: 0,
		FloorOffset:    1,
	}
	result, err := pl.ApplyRetention(policy)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 0 {
		t.Fatalf("floor offset should block all deletion, deleted=%d", result.DeletedSegments)
	}
	if countSegmentFiles(t, dir) != len(snap.segments) {
		t.Fatal("segments removed despite floor offset")
	}
}

func TestRetentionSafeOffset(t *testing.T) {
	got, enforce := retentionSafeOffset(100, RetentionPolicy{SafetyMessages: 30})
	if enforce != true || got != 70 {
		t.Fatalf("expected enforce=true safe=70, got enforce=%v safe=%d", enforce, got)
	}
	got, enforce = retentionSafeOffset(100, RetentionPolicy{FloorOffset: 80})
	if enforce != true || got != 80 {
		t.Fatalf("expected enforce=true safe=80, got enforce=%v safe=%d", enforce, got)
	}
	_, enforce = retentionSafeOffset(100, RetentionPolicy{})
	if enforce {
		t.Fatal("expected no offset enforcement when safety and floor are unset")
	}
}

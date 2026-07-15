package logcompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/ingestion/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactorRunOnce_localFilesystem(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")

	segmentPath := filepath.Join(sourceDir, "segment_test.log")
	var src bytesSegment
	for i := 0; i < 500; i++ {
		src.appendRecord(t, &pb.AdStreamEvent{
			EventType: []byte("impression"),
			ClickId:   []byte("imp-" + itoa(i)),
		})
	}
	src.appendRecord(t, &pb.AdStreamEvent{
		EventType: []byte("click"),
		ClickId:   []byte("click-1"),
	})
	require.NoError(t, os.WriteFile(segmentPath, src.bytes, 0o644))
	oldTime := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(segmentPath, oldTime, oldTime))

	store := NewLocalTierStore(sourceDir, warmDir)
	checkpoint := NewCheckpointStore(checkpointPath)
	compactor := NewCompactor(Config{
		HotMinAge:  time.Hour,
		SampleRate: 1000,
		WarmDir:    warmDir,
		SourceDir:  sourceDir,
	}, store, checkpoint, nil)

	require.NoError(t, compactor.RunOnce(context.Background()))

	destKey := warmDestKey("segment_test.log")
	destPath := filepath.Join(warmDir, destKey)
	assert.FileExists(t, destPath)

	plain, err := ReadWarm(destPath)
	require.NoError(t, err)
	assert.NotEmpty(t, plain)

	require.NoError(t, checkpoint.Load())
	record, ok := checkpoint.Get("segment_test.log")
	require.True(t, ok)
	assert.Equal(t, destKey, record.DestKey)
	assert.Equal(t, int64(501), record.OriginalCount)
	assert.Positive(t, record.KeptCount)
}

func TestMemoryS3TierStore_compaction(t *testing.T) {
	scratchDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	store := NewMemoryS3TierStore(scratchDir, "hot", "warm", NewMemoryObjectStore())

	store.SeedHot("segment_test.log", buildSegmentPayload(t, 100, 2), time.Now().Add(-48*time.Hour))

	compactor := NewCompactor(Config{
		HotMinAge: 0,
		WarmDir:   store.local.WarmDir,
		SourceDir: store.local.SourceDir,
	}, store, NewCheckpointStore(checkpointPath), nil)

	require.NoError(t, compactor.RunOnce(context.Background()))
	assert.Equal(t, 1, store.WarmObjectCount())
}

func TestS3TierStore_requiresConfig(t *testing.T) {
	_, err := NewS3TierStore(context.Background(), S3Config{Region: "eu-west-1"})
	require.ErrorIs(t, err, ErrCloudConfigIncomplete)
}

type bytesSegment struct {
	bytes []byte
}

func (seg *bytesSegment) appendRecord(t *testing.T, evt *pb.AdStreamEvent) {
	t.Helper()
	seg.bytes = append(seg.bytes, encodeRecord(t, evt)...)
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[pos:])
}

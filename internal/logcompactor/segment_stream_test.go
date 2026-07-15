package logcompactor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/ingestion/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterSegmentStream_matchesBufferFilter(t *testing.T) {
	var src bytes.Buffer
	for i := 0; i < 2000; i++ {
		src.Write(encodeRecord(t, &pb.AdStreamEvent{
			EventType: []byte("impression"),
			ClickId:   []byte("imp-" + itoa(i)),
		}))
	}
	src.Write(encodeRecord(t, &pb.AdStreamEvent{
		EventType: []byte("click"),
		ClickId:   []byte("billable"),
	}))

	var bufOut bytes.Buffer
	bufStats, err := filterSegment(src.Bytes(), 1000, &bufOut)
	require.NoError(t, err)

	var streamOut bytes.Buffer
	streamStats, err := filterSegmentStream(bytes.NewReader(src.Bytes()), 1000, &streamOut)
	require.NoError(t, err)

	assert.Equal(t, bufStats, streamStats)
	assert.Equal(t, bufOut.Bytes(), streamOut.Bytes())
}

func TestFilterSegmentStream_largeSegmentUsesStreamingPath(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	segmentPath := filepath.Join(sourceDir, "segment_stream.log")

	file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	for i := 0; i < 5000; i++ {
		_, err := file.Write(encodeRecord(t, &pb.AdStreamEvent{
			EventType: []byte("impression"),
			ClickId:   []byte("stream-" + itoa(i)),
		}))
		require.NoError(t, err)
	}
	require.NoError(t, file.Close())

	oldTime := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(segmentPath, oldTime, oldTime))

	store := NewLocalTierStore(sourceDir, warmDir)
	checkpoint := NewCheckpointStore(filepath.Join(t.TempDir(), "cp.jsonl"))
	compactor := NewCompactor(Config{HotMinAge: 0, WarmDir: warmDir, SourceDir: sourceDir}, store, checkpoint, nil)
	require.NoError(t, compactor.RunOnce(context.Background()))

	record, ok := checkpoint.Get("segment_stream.log")
	require.True(t, ok)
	assert.Equal(t, int64(5000), record.OriginalCount)
	assert.Positive(t, record.KeptCount)
	assert.NotEmpty(t, record.DestSHA256)
}

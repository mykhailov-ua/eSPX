package logcompactor

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpointStore_saveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	store := NewCheckpointStore(path)

	record := CheckpointRecord{
		SourceKey:     "segment_a.log",
		DestKey:       "segment_a.compact.zst",
		SourceSHA256:  "abc",
		OriginalCount: 1000,
		KeptCount:     10,
		CompactedAt:   time.Now().UTC(),
	}
	require.NoError(t, store.Save(record))

	reloaded := NewCheckpointStore(path)
	require.NoError(t, reloaded.Load())
	got, ok := reloaded.Get("segment_a.log")
	require.True(t, ok)
	assert.Equal(t, record.DestKey, got.DestKey)
	assert.Equal(t, record.KeptCount, got.KeptCount)
}

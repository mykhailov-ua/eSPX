package logcompactor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Guards compactor writes a pipeline marker before evacuator may upload the hot segment.
func TestChaos_logCompactorPipelineMarker(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")

	writeHotSegment(t, sourceDir, "segment_pipeline.log.zst.ready", buildSegmentPayload(t, 16, 1))

	store := NewLocalTierStore(sourceDir, warmDir)
	checkpoint := NewCheckpointStore(checkpointPath)
	compactor := NewCompactor(Config{
		HotMinAge: 0,
		WarmDir:   warmDir,
		SourceDir: sourceDir,
	}, store, checkpoint, nil)

	require.NoError(t, compactor.RunOnce(context.Background()))

	readyPath := filepath.Join(sourceDir, "segment_pipeline.log.zst.ready")
	require.True(t, CompactMarkerReady(readyPath))

	markerPath := CompactMarkerPath(sourceDir, "segment_pipeline.log.zst.ready")
	marker, err := readCompactMarker(markerPath)
	require.NoError(t, err)
	require.NotEmpty(t, marker.DestSHA256)

	logChaosProof(t, "log_compactor_pipeline_marker", map[string]string{
		"subsystem":       "log_compactor",
		"marker_written":  "true",
		"evacuator_ready": "true",
	})
}

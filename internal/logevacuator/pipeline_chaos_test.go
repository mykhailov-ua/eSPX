package logevacuator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/logcompactor"

	"github.com/stretchr/testify/require"
)

// Guards evacuator skips ready segments until compactor marker exists when ordering is enabled.
func TestChaos_logEvacuatorWaitsForCompactorMarker(t *testing.T) {
	logDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	memory := NewMemoryStore()

	evac, err := NewEvacuator(Config{
		LogDir:                 logDir,
		CheckpointPath:         checkpointPath,
		ScanInterval:           time.Hour,
		RequireCompactorMarker: true,
	}, memory)
	if err != nil {
		t.Fatalf("new evacuator: %v", err)
	}

	payload := []byte("pipeline-ordering-payload")
	readyPath := writeReadySegment(t, logDir, "segment_pipeline", payload)

	ctx := context.Background()
	require.NoError(t, evac.scanReadySegments(ctx))
	if memory.ObjectCount() != 0 {
		t.Fatalf("expected no upload before marker, got %d objects", memory.ObjectCount())
	}

	markerPath := logcompactor.CompactMarkerPath(logDir, filepath.Base(readyPath))
	marker := logcompactor.CompactMarker{
		SourceKey:    filepath.Base(readyPath),
		SourceSHA256: "test",
		DestKey:      "segment_pipeline.compact.zst",
		DestSHA256:   "dest",
		KeptCount:    1,
	}
	data, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(markerPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	require.NoError(t, evac.scanReadySegments(ctx))
	if memory.ObjectCount() != 1 {
		t.Fatalf("expected one upload after marker, got %d", memory.ObjectCount())
	}

	logChaosProof(t, "log_evacuator_waits_for_compactor_marker", map[string]string{
		"subsystem":             "log_evacuator",
		"skipped_before_marker": "true",
		"uploaded_after_marker": "true",
	})
}

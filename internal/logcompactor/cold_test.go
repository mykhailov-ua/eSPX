package logcompactor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/ingestion/pb"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregateWarmSegment_hourlyRollups(t *testing.T) {
	campaignID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	hour := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Hour)

	var plain bytes.Buffer
	for i := 0; i < 5; i++ {
		plain.Write(encodeRecord(t, &pb.AdStreamEvent{
			CampaignId:    campaignID[:],
			EventType:     []byte("impression"),
			ClickId:       []byte("imp-" + itoa(i)),
			CreatedAtUnix: hour.Add(time.Duration(i) * time.Second).Unix(),
		}))
	}
	plain.Write(encodeRecord(t, &pb.AdStreamEvent{
		CampaignId:    campaignID[:],
		EventType:     []byte("click"),
		ClickId:       []byte("click-1"),
		CreatedAtUnix: hour.Unix(),
		FraudScore:    1,
	}))

	rows, err := aggregateWarmSegment(bytes.NewReader(plain.Bytes()), "seg.compact.zst", "abc123")
	require.NoError(t, err)
	require.Len(t, rows, 2)

	var impressions, clicks RollupRow
	for _, row := range rows {
		switch row.EventType {
		case "impression":
			impressions = row
		case "click":
			clicks = row
		}
	}
	assert.Equal(t, uint64(5), impressions.EventCount)
	assert.Equal(t, hour, impressions.RollupHour)
	assert.Equal(t, uint64(1), clicks.EventCount)
	assert.Equal(t, uint64(1), clicks.FraudEventCount)
	assert.Equal(t, uint64(1), clicks.BillableEventCount)
}

func TestColdRolluperRunOnce_memoryInserter(t *testing.T) {
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "cold.jsonl")
	campaignID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	hour := time.Date(2025, 5, 15, 12, 0, 0, 0, time.UTC)

	var plain bytes.Buffer
	for i := 0; i < 20; i++ {
		plain.Write(encodeRecord(t, &pb.AdStreamEvent{
			CampaignId:    campaignID[:],
			EventType:     []byte("impression"),
			ClickId:       []byte("c-" + itoa(i)),
			CreatedAtUnix: hour.Add(time.Duration(i) * time.Minute).Unix(),
		}))
	}

	store := NewLocalTierStore("", warmDir)
	destKey := "segment_test.compact.zst"
	require.NoError(t, store.WriteWarm(context.Background(), destKey, plain.Bytes(), CompactionMeta{
		SourceKey: "segment_test.log",
		DestKey:   destKey,
	}))

	destPath := filepath.Join(warmDir, destKey)
	oldTime := time.Now().Add(-10 * 24 * time.Hour)
	require.NoError(t, os.Chtimes(destPath, oldTime, oldTime))

	inserter := &MemoryRollupInserter{}
	cr := NewColdRolluper(ColdConfig{
		WarmMinAge: 7 * 24 * time.Hour,
		WarmDir:    warmDir,
	}, store, NewCheckpointStore(checkpointPath), inserter)

	require.NoError(t, cr.RunOnce(context.Background()))
	require.NotEmpty(t, inserter.Rows)
	assert.Equal(t, uint64(20), inserter.Rows[0].EventCount)

	record, ok := cr.checkpoint.Get(destKey)
	require.True(t, ok)
	assert.Equal(t, destKey, record.SourceKey)
}

func TestRefreshHotLag_pendingSegments(t *testing.T) {
	sourceDir := t.TempDir()
	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.jsonl")

	segmentPath := filepath.Join(sourceDir, "segment_lag_test.log")
	require.NoError(t, os.WriteFile(segmentPath, []byte("x"), 0o644))
	oldTime := time.Now().Add(-72 * time.Hour)
	require.NoError(t, os.Chtimes(segmentPath, oldTime, oldTime))

	store := NewLocalTierStore(sourceDir, warmDir)
	checkpoint := NewCheckpointStore(checkpointPath)
	require.NoError(t, checkpoint.Load())

	RegisterMetrics()
	refreshHotLag(context.Background(), store, checkpoint)
	assert.Equal(t, float64(1), testutil.ToFloat64(hotPendingTotal))
	assert.InDelta(t, 72*3600, testutil.ToFloat64(hotLagSeconds), 5)
}

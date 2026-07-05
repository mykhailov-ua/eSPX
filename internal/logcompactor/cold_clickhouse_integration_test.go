package logcompactor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"espx/internal/ads/pb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseRollupInserter_insert_RealCH(t *testing.T) {
	conn, cleanup := setupClickHouseIntegration(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	campaignID := uuid.MustParse("33333333-4444-5555-6666-777777777777")
	hour := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	inserter := NewClickHouseRollupInserter(conn)

	require.NoError(t, inserter.InsertRollups(ctx, []RollupRow{{
		RollupHour:         hour,
		CampaignID:         campaignID,
		EventType:          "impression",
		EventCount:         42,
		FraudEventCount:    2,
		BillableEventCount: 0,
		SampleClickIDs:     []string{"click-1"},
		SourceSegment:      "direct_insert.compact.zst",
		WarmDestSHA256:     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}}))

	var rowCount uint64
	require.NoError(t, conn.QueryRow(ctx, `SELECT count() FROM ad_event_processor.audit_log_rollups`).Scan(&rowCount))
	require.Equal(t, uint64(1), rowCount)
}

func TestColdRolluper_ClickHouse_RealCH(t *testing.T) {
	conn, cleanup := setupClickHouseIntegration(t)
	defer cleanup()

	warmDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "cold.jsonl")
	campaignID := uuid.MustParse("22222222-3333-4444-5555-666666666666")
	hour := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Hour)
	destKey := "segment_ch_test.compact.zst"

	var plain bytesSegment
	for i := 0; i < 100; i++ {
		plain.appendRecord(t, &pb.AdStreamEvent{
			CampaignId:    campaignID[:],
			EventType:     []byte("impression"),
			ClickId:       []byte("imp-" + itoa(i)),
			CreatedAtUnix: hour.Add(time.Duration(i) * time.Second).Unix(),
		})
	}
	plain.appendRecord(t, &pb.AdStreamEvent{
		CampaignId:    campaignID[:],
		EventType:     []byte("click"),
		ClickId:       []byte("click-ch-1"),
		CreatedAtUnix: hour.Unix(),
	})

	store := NewLocalTierStore("", warmDir)
	require.NoError(t, store.WriteWarm(context.Background(), destKey, plain.bytes, CompactionMeta{
		SourceKey: "segment_ch_test.log",
		DestKey:   destKey,
	}))

	destPath := filepath.Join(warmDir, destKey)
	oldTime := time.Now().Add(-10 * 24 * time.Hour)
	require.NoError(t, os.Chtimes(destPath, oldTime, oldTime))

	inserter := NewClickHouseRollupInserter(conn)
	cr := NewColdRolluper(ColdConfig{
		WarmMinAge: 7 * 24 * time.Hour,
		WarmDir:    warmDir,
	}, store, NewCheckpointStore(checkpointPath), inserter)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var tableExists uint64
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT count()
		FROM system.tables
		WHERE database = 'ad_event_processor' AND name = 'audit_log_rollups'`).Scan(&tableExists))
	require.Equal(t, uint64(1), tableExists)

	objects, err := store.ListWarm(ctx, time.Now().Add(-cr.cfg.WarmMinAge))
	require.NoError(t, err)
	require.Len(t, objects, 1, "warm segment must be eligible for cold rollup")

	warmPlain, err := ReadWarm(destPath)
	require.NoError(t, err)
	fileDigest, err := computeFileDigest(destPath)
	require.NoError(t, err)
	aggRows, err := aggregateWarmSegment(bytes.NewReader(warmPlain), destKey, fileDigest.SHA256)
	require.NoError(t, err)
	require.NotEmpty(t, aggRows)
	for _, row := range aggRows {
		assert.Positive(t, row.EventCount)
	}

	require.NoError(t, cr.RunOnce(ctx))
	require.NoError(t, cr.RunOnce(ctx), "second pass must be idempotent")

	record, ok := cr.checkpoint.Get(destKey)
	require.True(t, ok, "cold checkpoint must be written after rollup")
	assert.Equal(t, int64(2), record.KeptCount)

	var rowCount uint64
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT count()
		FROM ad_event_processor.audit_log_rollups
		WHERE source_segment = ?`, destKey).Scan(&rowCount))
	assert.Equal(t, uint64(2), rowCount, "impression + click buckets")

	var impressionEvents uint64
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT sum(event_count)
		FROM ad_event_processor.audit_log_rollups
		WHERE source_segment = ? AND event_type = 'impression'`, destKey).Scan(&impressionEvents))
	assert.Equal(t, uint64(100), impressionEvents)
}

func TestAuditLogRollups_Explain_RealCH(t *testing.T) {
	conn, cleanup := setupClickHouseIntegration(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS tmp_audit_log_rollups_seed AS audit_log_rollups
		ENGINE = MergeTree
		PARTITION BY toYYYYMM(rollup_hour)
		ORDER BY (campaign_id, rollup_hour, event_type, source_segment, warm_dest_sha256)`))
	require.NoError(t, conn.Exec(ctx, `TRUNCATE TABLE tmp_audit_log_rollups_seed`))
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO tmp_audit_log_rollups_seed (
			rollup_hour, campaign_id, event_type,
			event_count, fraud_event_count, billable_event_count,
			sample_click_ids, source_segment, warm_dest_sha256
		)
		SELECT
			toStartOfHour(now() - toIntervalHour(number % 168)) AS rollup_hour,
			toUUID(concat('00000000-0000-4000-8000-', lpad(toString(number % 200), 12, '0'))) AS campaign_id,
			['impression', 'click', 'conversion', 'fraud_mark'][1 + (number % 4)] AS event_type,
			50 + (number % 100) AS event_count,
			number % 7 AS fraud_event_count,
			40 + (number % 80) AS billable_event_count,
			[concat('click-', toString(number % 1000))] AS sample_click_ids,
			concat('segment_', toString(number % 500), '.compact.zst') AS source_segment,
			lower(hex(SHA256(toString(number)))) AS warm_dest_sha256
		FROM numbers(100000)`))

	queries := []struct {
		name  string
		query string
	}{
		{
			name: "campaign_hourly_volume_7d",
			query: `
				SELECT
					rollup_hour,
					event_type,
					sum(event_count) AS events,
					sum(fraud_event_count) AS fraud_events
				FROM tmp_audit_log_rollups_seed
				WHERE campaign_id = toUUID('00000000-0000-4000-8000-000000000042')
				  AND rollup_hour >= now() - INTERVAL 7 DAY
				GROUP BY rollup_hour, event_type
				ORDER BY rollup_hour DESC`,
		},
		{
			name: "fraud_rate_by_campaign_30d",
			query: `
				SELECT
					campaign_id,
					sum(event_count) AS total_events,
					sum(fraud_event_count) AS fraud_events,
					if(sum(event_count) = 0, 0, sum(fraud_event_count) / sum(event_count)) AS fraud_rate
				FROM tmp_audit_log_rollups_seed
				WHERE rollup_hour >= now() - INTERVAL 30 DAY
				GROUP BY campaign_id
				HAVING total_events > 1000
				ORDER BY fraud_rate DESC
				LIMIT 50`,
		},
		{
			name: "top_billable_campaigns_24h",
			query: `
				SELECT
					campaign_id,
					sum(billable_event_count) AS billable_events
				FROM tmp_audit_log_rollups_seed
				WHERE rollup_hour >= now() - INTERVAL 24 HOUR
				GROUP BY campaign_id
				ORDER BY billable_events DESC
				LIMIT 20`,
		},
		{
			name: "monthly_partition_prune",
			query: `
				SELECT
					toStartOfMonth(rollup_hour) AS month,
					sum(event_count) AS events
				FROM tmp_audit_log_rollups_seed
				WHERE rollup_hour >= now() - INTERVAL 90 DAY
				  AND rollup_hour < now()
				GROUP BY month
				ORDER BY month`,
		},
	}

	for _, q := range queries {
		plan := queryCHExplainPlan(t, conn, q.query)
		assert.Contains(t, plan, "MergeTree", "query %s should use MergeTree index", q.name)
		logChaosProof(t, "log_compactor_ch_explain_"+q.name, map[string]string{
			"subsystem": "log_compactor",
			"summary":   summarizeExplainPlan(plan),
		})
	}

	require.NoError(t, conn.Exec(ctx, `DROP TABLE tmp_audit_log_rollups_seed`))
}

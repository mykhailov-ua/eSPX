package logcompactor

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// RollupInserter persists cold-tier rollup rows.
type RollupInserter interface {
	InsertRollups(ctx context.Context, rows []RollupRow) error
}

// ClickHouseRollupInserter writes rollups to ad_event_processor.audit_log_rollups.
type ClickHouseRollupInserter struct {
	conn driver.Conn
}

// NewClickHouseRollupInserter wraps a ClickHouse connection for cold-tier inserts.
func NewClickHouseRollupInserter(conn driver.Conn) *ClickHouseRollupInserter {
	return &ClickHouseRollupInserter{conn: conn}
}

// InsertRollups batch-inserts rollup rows.
func (inserter *ClickHouseRollupInserter) InsertRollups(ctx context.Context, rows []RollupRow) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := inserter.conn.PrepareBatch(ctx, `
		INSERT INTO ad_event_processor.audit_log_rollups (
			rollup_hour, campaign_id, event_type,
			event_count, fraud_event_count, billable_event_count,
			sample_click_ids, source_segment, warm_dest_sha256
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare audit_log_rollups batch: %w", err)
	}

	for _, row := range rows {
		if err := batch.Append(
			row.RollupHour,
			row.CampaignID,
			row.EventType,
			row.EventCount,
			row.FraudEventCount,
			row.BillableEventCount,
			row.SampleClickIDs,
			row.SourceSegment,
			row.WarmDestSHA256,
		); err != nil {
			return fmt.Errorf("append rollup row: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send audit_log_rollups batch: %w", err)
	}
	return nil
}

// MemoryRollupInserter stores rollups in-process for tests.
type MemoryRollupInserter struct {
	Rows []RollupRow
}

// InsertRollups appends rows to the in-memory sink.
func (inserter *MemoryRollupInserter) InsertRollups(_ context.Context, rows []RollupRow) error {
	inserter.Rows = append(inserter.Rows, rows...)
	return nil
}

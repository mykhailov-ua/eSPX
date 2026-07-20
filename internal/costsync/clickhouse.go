package costsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// ClickHouseInserter writes cost snapshot rows for M17 placement stats MV.
type ClickHouseInserter struct {
	conn driver.Conn
}

// NewClickHouseInserter wraps a ClickHouse connection.
func NewClickHouseInserter(conn driver.Conn) *ClickHouseInserter {
	return &ClickHouseInserter{conn: conn}
}

// InsertSnapshots batch-inserts normalized cost lines into cost_snapshots.
func (inserter *ClickHouseInserter) InsertSnapshots(ctx context.Context, lines []CostLine, usdMicro []int64) error {
	if inserter == nil || inserter.conn == nil || len(lines) == 0 {
		return nil
	}

	batch, err := inserter.conn.PrepareBatch(ctx, `
		INSERT INTO ad_event_processor.cost_snapshots (
			snapshot_hour, customer_id, campaign_id, network, placement_id, line_type, amount_usd_micro
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare cost_snapshots batch: %w", err)
	}

	for i, line := range lines {
		hour := time.Date(line.Date.Year(), line.Date.Month(), line.Date.Day(), 0, 0, 0, 0, time.UTC)
		amount := line.AmountMicro
		if i < len(usdMicro) {
			amount = usdMicro[i]
		}
		if err := batch.Append(hour, line.CustomerID, line.CampaignID, line.Network, line.PlacementID, string(line.LineType), amount); err != nil {
			return fmt.Errorf("append cost snapshot: %w", err)
		}
	}
	return batch.Send()
}

// MemorySnapshotInserter stores snapshots in-process for tests.
type MemorySnapshotInserter struct {
	Rows []struct {
		CampaignID  uuid.UUID
		PlacementID string
		AmountMicro int64
		LineType    LineType
	}
}

// InsertSnapshots records rows in memory.
func (m *MemorySnapshotInserter) InsertSnapshots(_ context.Context, lines []CostLine, usdMicro []int64) error {
	for i, line := range lines {
		amount := line.AmountMicro
		if i < len(usdMicro) {
			amount = usdMicro[i]
		}
		m.Rows = append(m.Rows, struct {
			CampaignID  uuid.UUID
			PlacementID string
			AmountMicro int64
			LineType    LineType
		}{
			CampaignID:  line.CampaignID,
			PlacementID: line.PlacementID,
			AmountMicro: amount,
			LineType:    line.LineType,
		})
	}
	return nil
}

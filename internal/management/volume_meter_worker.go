package management

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	billingdb "espx/internal/billing/db"
	"espx/internal/database"
	"espx/internal/licensing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const meterBillableEvents = "events"

// VolumeMeterWorker rolls up weighted billable units from ClickHouse into usage_meters.
type VolumeMeterWorker struct {
	pool     *pgxpool.Pool
	ch       *database.CHQuery
	interval time.Duration
	pgGate   *MgmtPgGate
}

// NewVolumeMeterWorker constructs an hourly CH rollup worker.
func NewVolumeMeterWorker(pool *pgxpool.Pool, ch *database.CHQuery, interval time.Duration, pgGate *MgmtPgGate) *VolumeMeterWorker {
	if interval <= 0 {
		interval = time.Hour
	}
	return &VolumeMeterWorker{pool: pool, ch: ch, interval: interval, pgGate: pgGate}
}

// Start runs the rollup loop until ctx is cancelled.
func (w *VolumeMeterWorker) Start(ctx context.Context) {
	if w == nil || w.pool == nil || w.ch == nil {
		return
	}
	slog.Info("volume meter worker starting", "interval", w.interval)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.RunHour(ctx, time.Now().UTC()); err != nil {
				slog.Error("volume meter rollup failed", "error", err)
			}
		}
	}
}

type rollupRow struct {
	CampaignID uuid.UUID
	EventType  string
	Count      uint64
}

// RunHour aggregates the previous clock hour into billing.usage_meters.
func (w *VolumeMeterWorker) RunHour(ctx context.Context, now time.Time) error {
	if w.pgGate != nil {
		if err := w.pgGate.AcquireLow(ctx); err != nil {
			return err
		}
		defer w.pgGate.ReleaseLow()
	}
	return w.runHour(ctx, now)
}

func (w *VolumeMeterWorker) runHour(ctx context.Context, now time.Time) error {
	hourEnd := now.Truncate(time.Hour)
	hourStart := hourEnd.Add(-time.Hour)

	rows, err := w.queryRollups(ctx, hourStart, hourEnd)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	campaignCustomers, err := w.loadCampaignCustomers(ctx)
	if err != nil {
		return err
	}

	customerUnits := make(map[uuid.UUID]int64)
	for _, row := range rows {
		custID, ok := campaignCustomers[row.CampaignID]
		if !ok {
			continue
		}
		cat := licensing.ClassifyEventType(row.EventType)
		units := int64(row.Count) * licensing.BillableWeightPermille(cat) / 1000
		customerUnits[custID] += units
	}

	period := time.Date(hourStart.Year(), hourStart.Month(), 1, 0, 0, 0, 0, time.UTC)
	q := billingdb.New(w.pool)
	for custID, units := range customerUnits {
		if units <= 0 {
			continue
		}
		if _, err := q.IncrementUsageMeter(ctx, billingdb.IncrementUsageMeterParams{
			CustomerID: pgtype.UUID{Bytes: custID, Valid: true},
			Meter:      meterBillableEvents,
			Period:     pgtype.Date{Time: period, Valid: true},
			Value:      units,
		}); err != nil {
			return fmt.Errorf("increment usage meter customer=%s: %w", custID, err)
		}
	}
	slog.Info("volume meter rollup complete",
		"hour", hourStart.Format(time.RFC3339),
		"customers", len(customerUnits),
	)
	return nil
}

func (w *VolumeMeterWorker) queryRollups(ctx context.Context, from, to time.Time) ([]rollupRow, error) {
	const q = `
		SELECT
			campaign_id,
			event_type,
			sum(event_count) AS cnt
		FROM ad_event_processor.audit_log_rollups
		WHERE rollup_hour >= ? AND rollup_hour < ?
		GROUP BY campaign_id, event_type`

	chRows, err := w.ch.Query(ctx, q, from, to)
	if err != nil {
		return nil, fmt.Errorf("clickhouse rollup query: %w", err)
	}
	defer chRows.Close()

	var out []rollupRow
	for chRows.Next() {
		var row rollupRow
		if err := chRows.Scan(&row.CampaignID, &row.EventType, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, chRows.Err()
}

func (w *VolumeMeterWorker) loadCampaignCustomers(ctx context.Context) (map[uuid.UUID]uuid.UUID, error) {
	pgRows, err := w.pool.Query(ctx, `SELECT id, customer_id FROM campaigns`)
	if err != nil {
		return nil, err
	}
	defer pgRows.Close()

	out := make(map[uuid.UUID]uuid.UUID)
	for pgRows.Next() {
		var campID, custID uuid.UUID
		if err := pgRows.Scan(&campID, &custID); err != nil {
			return nil, err
		}
		out[campID] = custID
	}
	return out, pgRows.Err()
}

// ComputeWeightedUnitsFromRows is exported for golden-fixture tests.
func ComputeWeightedUnitsFromRows(rows []rollupRow, campaignCustomers map[uuid.UUID]uuid.UUID) map[uuid.UUID]int64 {
	customerUnits := make(map[uuid.UUID]int64)
	for _, row := range rows {
		custID, ok := campaignCustomers[row.CampaignID]
		if !ok {
			continue
		}
		cat := licensing.ClassifyEventType(row.EventType)
		units := int64(row.Count) * licensing.BillableWeightPermille(cat) / 1000
		customerUnits[custID] += units
	}
	return customerUnits
}

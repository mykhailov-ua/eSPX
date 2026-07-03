package management

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/ui/dashboard"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	dashboardMetricsTimeout = 2 * time.Second
	dashboardChartPoints    = 48
)

// dashboardMetricsSource loads dashboard view-model data for the fan-out worker.
type dashboardMetricsSource interface {
	Collect(ctx context.Context, now time.Time) (dashboard.Snapshot, error)
}

// DashboardMetricsCollector reads Postgres, Redis, and optional ClickHouse on the worker tick only.
type DashboardMetricsCollector struct {
	svc         *Service
	ch          driver.Conn
	streamName  string
	fraudStream string

	mu            sync.Mutex
	lastStreamLen int64
	lastFraudLen  int64
	lastSampleAt  time.Time
	rateHistory   [dashboardChartPoints]float64
	labelHistory  [dashboardChartPoints]string
	historyIdx    int
	historyCount  int
}

// NewDashboardMetricsCollector wires cold-path stores used by the dashboard fan-out worker.
func NewDashboardMetricsCollector(svc *Service, ch driver.Conn, cfg *config.Config) *DashboardMetricsCollector {
	streamName := "ad:events:stream"
	fraudStream := "ad:events:fraud"
	if cfg != nil {
		if cfg.RedisStreamName != "" {
			streamName = cfg.RedisStreamName
		}
		if cfg.FraudStreamName != "" {
			fraudStream = cfg.FraudStreamName
		}
	}
	return &DashboardMetricsCollector{
		svc:         svc,
		ch:          ch,
		streamName:  streamName,
		fraudStream: fraudStream,
	}
}

// Collect builds a dashboard snapshot from cached backend reads.
func (c *DashboardMetricsCollector) Collect(ctx context.Context, now time.Time) (dashboard.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, dashboardMetricsTimeout)
	defer cancel()

	var (
		activeCampaigns int64
		pendingOutbox   int64
		events          []dashboard.EventRow
		streamLen       int64
		fraudLen        int64
		chLabels        []string
		chValues        []float64
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		q := db.New(c.svc.pool)
		status := pgtype.Text{String: "ACTIVE", Valid: true}
		n, err := q.CountCampaigns(gctx, db.CountCampaignsParams{Status: status})
		if err != nil {
			return fmt.Errorf("count active campaigns: %w", err)
		}
		activeCampaigns = n
		return nil
	})

	g.Go(func() error {
		n, err := db.New(c.svc.pool).CountPendingOutboxEvents(gctx)
		if err != nil {
			return fmt.Errorf("count pending outbox: %w", err)
		}
		pendingOutbox = n
		return nil
	})

	g.Go(func() error {
		rows, err := db.New(c.svc.pool).ListAuditLogs(gctx, db.ListAuditLogsParams{
			Limit:  5,
			Offset: 0,
		})
		if err != nil {
			return fmt.Errorf("list audit logs: %w", err)
		}
		events = auditRowsToEvents(rows, now)
		return nil
	})

	g.Go(func() error {
		rdb := c.redis()
		if rdb == nil {
			return nil
		}
		mainLen, err := rdb.XLen(gctx, c.streamName).Result()
		if err != nil && err != redis.Nil {
			return fmt.Errorf("redis stream len %q: %w", c.streamName, err)
		}
		streamLen = mainLen

		fLen, err := rdb.XLen(gctx, c.fraudStream).Result()
		if err != nil && err != redis.Nil {
			return fmt.Errorf("redis fraud stream len %q: %w", c.fraudStream, err)
		}
		fraudLen = fLen
		return nil
	})

	if c.ch != nil {
		g.Go(func() error {
			labels, values, err := c.queryClickHouseRates(gctx, now)
			if err != nil {
				slog.Warn("dashboard clickhouse rate query failed", "error", err)
				return nil
			}
			chLabels = labels
			chValues = values
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return dashboard.Snapshot{}, err
	}

	reqPerSec, errRate := c.ingestRates(streamLen, fraudLen, now)

	chart := c.chartFromSources(now, chLabels, chValues, reqPerSec)
	if len(events) == 0 {
		events = defaultRecentEvents(now)
	}

	return dashboard.Snapshot{
		RequestsPerSec:  fmt.Sprintf("%.0f", reqPerSec),
		ActiveCampaigns: fmt.Sprintf("%d", activeCampaigns),
		ErrorRate:       fmt.Sprintf("%.2f%%", errRate),
		PendingOutbox:   fmt.Sprintf("%d", pendingOutbox),
		UpdatedAt:       now,
		TrafficChart:    chart,
		RecentEvents:    events,
	}, nil
}

func (c *DashboardMetricsCollector) redis() redis.UniversalClient {
	if c.svc == nil || len(c.svc.rdbs) == 0 {
		return nil
	}
	return c.svc.rdbs[0]
}

func (c *DashboardMetricsCollector) ingestRates(streamLen, fraudLen int64, now time.Time) (reqPerSec float64, errRate float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.lastSampleAt.IsZero() {
		elapsed := now.Sub(c.lastSampleAt).Seconds()
		if elapsed > 0 {
			mainDelta := float64(streamLen - c.lastStreamLen)
			if mainDelta < 0 {
				mainDelta = 0
			}
			reqPerSec = mainDelta / elapsed

			fraudDelta := float64(fraudLen - c.lastFraudLen)
			if fraudDelta < 0 {
				fraudDelta = 0
			}
			total := mainDelta + fraudDelta
			if total > 0 {
				errRate = (fraudDelta / total) * 100
			}
		}
	}

	c.pushRateSample(now, reqPerSec)
	c.lastStreamLen = streamLen
	c.lastFraudLen = fraudLen
	c.lastSampleAt = now
	return reqPerSec, errRate
}

func (c *DashboardMetricsCollector) pushRateSample(now time.Time, reqPerSec float64) {
	c.rateHistory[c.historyIdx] = reqPerSec
	c.labelHistory[c.historyIdx] = now.Format("15:04:05")
	c.historyIdx = (c.historyIdx + 1) % dashboardChartPoints
	if c.historyCount < dashboardChartPoints {
		c.historyCount++
	}
}

func (c *DashboardMetricsCollector) chartFromSources(now time.Time, chLabels []string, chValues []float64, current float64) dashboard.ChartView {
	const title = "Ingest rate"

	if len(chLabels) > 0 && len(chValues) == len(chLabels) {
		labels := append([]string(nil), chLabels...)
		values := append([]float64(nil), chValues...)
		values[len(values)-1] = current
		return dashboard.BuildLineChart(title, labels, values)
	}

	labels, values := c.historySeries()
	if len(values) == 0 {
		chart := dashboard.BuildTrafficChart(now)
		if n := len(chart.Series[0].Values); n > 0 {
			chart.Series[0].Values[n-1] = current
		}
		return chart
	}

	values[len(values)-1] = current
	return dashboard.BuildLineChart(title, labels, values)
}

func (c *DashboardMetricsCollector) historySeries() (labels []string, values []float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.historyCount == 0 {
		return nil, nil
	}

	labels = make([]string, c.historyCount)
	values = make([]float64, c.historyCount)

	start := 0
	if c.historyCount == dashboardChartPoints {
		start = c.historyIdx
	}

	for i := 0; i < c.historyCount; i++ {
		idx := (start + i) % dashboardChartPoints
		labels[i] = c.labelHistory[idx]
		values[i] = c.rateHistory[idx]
	}
	return labels, values
}

const clickHouseEventRateQuery = `
SELECT
    toStartOfMinute(ts) AS bucket,
    toFloat64(count()) AS cnt
FROM (
    SELECT created_at AS ts FROM impressions WHERE created_at >= ?
    UNION ALL
    SELECT created_at AS ts FROM clicks WHERE created_at >= ?
) AS ev
GROUP BY bucket
ORDER BY bucket
`

func (c *DashboardMetricsCollector) queryClickHouseRates(ctx context.Context, now time.Time) ([]string, []float64, error) {
	since := now.Add(-time.Duration(dashboardChartPoints) * time.Minute)
	rows, err := c.ch.Query(ctx, clickHouseEventRateQuery, since, since)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	labels := make([]string, 0, dashboardChartPoints)
	values := make([]float64, 0, dashboardChartPoints)
	for rows.Next() {
		var bucket time.Time
		var cnt float64
		if err := rows.Scan(&bucket, &cnt); err != nil {
			return nil, nil, err
		}
		labels = append(labels, bucket.Format("15:04:05"))
		values = append(values, cnt/60.0)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return labels, values, nil
}

func auditRowsToEvents(rows []db.AdminAuditLog, now time.Time) []dashboard.EventRow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]dashboard.EventRow, 0, len(rows))
	for _, row := range rows {
		ts := now
		if row.CreatedAt.Valid {
			ts = row.CreatedAt.Time
		}
		target := ""
		if row.TargetID.Valid {
			if id, err := uuid.FromBytes(row.TargetID.Bytes[:]); err == nil {
				s := id.String()
				if len(s) >= 8 {
					target = s[:8]
				}
			}
		}
		detail := row.Action
		if target != "" {
			detail = row.Action + " " + target
		}
		out = append(out, dashboard.EventRow{
			Time:   ts.Format("15:04:05"),
			Type:   row.TargetType,
			Detail: detail,
		})
	}
	return out
}

func defaultRecentEvents(now time.Time) []dashboard.EventRow {
	return []dashboard.EventRow{
		{Time: now.Add(-2 * time.Minute).Format("15:04:05"), Type: "system", Detail: "no audit rows yet"},
	}
}

type syntheticDashboardSource struct{}

func (syntheticDashboardSource) Collect(_ context.Context, now time.Time) (dashboard.Snapshot, error) {
	return buildSyntheticSnapshot(now), nil
}

func buildSyntheticSnapshot(now time.Time) dashboard.Snapshot {
	n := now.Unix() % 1000
	traffic := dashboard.BuildTrafficChart(now)
	return dashboard.Snapshot{
		RequestsPerSec:  fmt.Sprintf("%d", 1200+(n%400)),
		ActiveCampaigns: fmt.Sprintf("%d", 42+(n%17)),
		ErrorRate:       fmt.Sprintf("%.2f%%", float64(n%50)/100.0),
		PendingOutbox:   fmt.Sprintf("%d", n%12),
		UpdatedAt:       now,
		TrafficChart:    traffic,
		RecentEvents: []dashboard.EventRow{
			{Time: now.Add(-2 * time.Minute).Format("15:04:05"), Type: "campaign", Detail: "pacing tick ok"},
			{Time: now.Add(-5 * time.Minute).Format("15:04:05"), Type: "audit", Detail: "settings read"},
			{Time: now.Add(-8 * time.Minute).Format("15:04:05"), Type: "delivery", Detail: "outbox flush"},
		},
	}
}

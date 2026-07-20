package database

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// WatchedPgTables are write-heavy relations monitored for autovacuum lag (M-DB-PG-6).
var WatchedPgTables = []string{"balance_ledger", "campaigns", "outbox_events"}

type pgTableStatsCollector struct {
	pool     *pgxpool.Pool
	deadDesc *prometheus.Desc
	liveDesc *prometheus.Desc
}

// NewPgTableStatsCollector exports n_dead_tup / n_live_tup for watched public tables.
func NewPgTableStatsCollector(pool *pgxpool.Pool) prometheus.Collector {
	return &pgTableStatsCollector{
		pool: pool,
		deadDesc: prometheus.NewDesc(
			"ad_pg_dead_tuples",
			"Dead tuple count from pg_stat_user_tables",
			[]string{"table"}, nil,
		),
		liveDesc: prometheus.NewDesc(
			"ad_pg_live_tuples",
			"Live tuple count from pg_stat_user_tables",
			[]string{"table"}, nil,
		),
	}
}

func (c *pgTableStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.deadDesc
	ch <- c.liveDesc
}

func (c *pgTableStatsCollector) Collect(ch chan<- prometheus.Metric) {
	if c == nil || c.pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := c.pool.Query(ctx, `
		SELECT relname, COALESCE(n_dead_tup, 0), COALESCE(n_live_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = 'public' AND relname = ANY($1)`, WatchedPgTables)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var dead, live int64
		if err := rows.Scan(&name, &dead, &live); err != nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.deadDesc, prometheus.GaugeValue, float64(dead), name)
		ch <- prometheus.MustNewConstMetric(c.liveDesc, prometheus.GaugeValue, float64(live), name)
	}
}

// QueryPgTableDeadTuples returns dead/live tuple counts for watched tables (tests/ops).
func QueryPgTableDeadTuples(ctx context.Context, pool *pgxpool.Pool) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `
		SELECT relname, COALESCE(n_dead_tup, 0)
		FROM pg_stat_user_tables
		WHERE schemaname = 'public' AND relname = ANY($1)`, WatchedPgTables)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]int64, len(WatchedPgTables))
	for rows.Next() {
		var name string
		var dead int64
		if err := rows.Scan(&name, &dead); err != nil {
			return nil, err
		}
		out[name] = dead
	}
	return out, rows.Err()
}

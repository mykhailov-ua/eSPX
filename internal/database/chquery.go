package database

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	defaultCHQueryMaxMemoryBytes = 1 << 30
	defaultCHQueryMaxExecSeconds = 30
	defaultCHStaleThreshold      = 5 * time.Minute
)

// CHQueryConfig governs read-only ClickHouse admin/report queries (CHG-*).
type CHQueryConfig struct {
	MaxMemoryBytes      uint64
	MaxExecutionTimeSec int
}

// CHQuery wraps ClickHouse reads with per-query memory and time caps.
type CHQuery struct {
	conn       driver.Conn
	maxMemory  uint64
	maxExecSec int
}

// NewCHQuery creates a governed read-only query client.
func NewCHQuery(conn driver.Conn, cfg CHQueryConfig) *CHQuery {
	maxMem := cfg.MaxMemoryBytes
	if maxMem == 0 {
		maxMem = defaultCHQueryMaxMemoryBytes
	}
	maxExec := cfg.MaxExecutionTimeSec
	if maxExec == 0 {
		maxExec = defaultCHQueryMaxExecSeconds
	}
	return &CHQuery{conn: conn, maxMemory: maxMem, maxExecSec: maxExec}
}

func (q *CHQuery) settings() clickhouse.Settings {
	return clickhouse.Settings{
		"readonly":           1,
		"max_memory_usage":   q.maxMemory,
		"max_execution_time": q.maxExecSec,
	}
}

func (q *CHQuery) withSettings(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(q.settings()))
}

// Query runs a governed read-only query.
func (q *CHQuery) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if q == nil || q.conn == nil {
		return nil, fmt.Errorf("chquery: no connection")
	}
	return q.conn.Query(q.withSettings(ctx), query, args...)
}

// QueryRow runs a governed read-only query returning one row.
func (q *CHQuery) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if q == nil || q.conn == nil {
		return &errRow{err: fmt.Errorf("chquery: no connection")}
	}
	return q.conn.QueryRow(q.withSettings(ctx), query, args...)
}

// Exec runs a governed statement (used in tests for SETTINGS validation).
func (q *CHQuery) Exec(ctx context.Context, query string, args ...any) error {
	if q == nil || q.conn == nil {
		return fmt.Errorf("chquery: no connection")
	}
	return q.conn.Exec(q.withSettings(ctx), query, args...)
}

// IngestionLag measures delay between now and the latest event timestamp in raw tables.
func (q *CHQuery) IngestionLag(ctx context.Context) (time.Duration, error) {
	var latest time.Time
	err := q.QueryRow(ctx, `
SELECT max(latest) FROM (
    SELECT max(created_at) AS latest FROM impressions
    UNION ALL
    SELECT max(created_at) FROM clicks
    UNION ALL
    SELECT max(created_at) FROM conversions
)`).Scan(&latest)
	if err != nil {
		return 0, err
	}
	if latest.IsZero() {
		return 0, nil
	}
	lag := time.Since(latest)
	if lag < 0 {
		return 0, nil
	}
	return lag, nil
}

// Freshness builds admin API freshness metadata from measured lag.
func Freshness(lag time.Duration, staleThreshold time.Duration) (stale bool, lagSeconds int) {
	if staleThreshold <= 0 {
		staleThreshold = defaultCHStaleThreshold
	}
	lagSeconds = int(lag.Seconds())
	if lagSeconds < 0 {
		lagSeconds = 0
	}
	return lag > staleThreshold, lagSeconds
}

type errRow struct {
	err error
}

func (r *errRow) Scan(dest ...any) error {
	return r.err
}

func (r *errRow) ScanStruct(dest any) error {
	return r.err
}

func (r *errRow) Err() error {
	return r.err
}

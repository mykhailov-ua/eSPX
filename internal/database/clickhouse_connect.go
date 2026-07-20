package database

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ConnectClickHouse opens an analytics connection tuned for async inserts so the processor never blocks on part commits.
func ConnectClickHouse(ctx context.Context, dsn string) (driver.Conn, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse clickhouse dsn: %w", err)
	}

	opts.Settings = clickhouse.Settings{
		"max_execution_time":    60,
		"async_insert":          1,
		"wait_for_async_insert": 1,
	}
	opts.DialTimeout = 5 * time.Second
	opts.ConnMaxLifetime = time.Hour

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	return conn, nil
}

// ConnectCHReadonly opens a read-only ClickHouse connection for cold-path queries (CHG-*).
// Does not enable async_insert; pair with database.CHQuery for per-query caps.
func ConnectCHReadonly(ctx context.Context, dsn string) (driver.Conn, error) {
	if dsn == "" {
		return nil, fmt.Errorf("clickhouse readonly dsn is empty")
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse clickhouse readonly dsn: %w", err)
	}
	opts.Settings = clickhouse.Settings{
		"readonly":           1,
		"max_execution_time": 60,
	}
	opts.DialTimeout = 5 * time.Second
	opts.ConnMaxLifetime = time.Hour

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse readonly connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to ping clickhouse readonly: %w", err)
	}
	return conn, nil
}

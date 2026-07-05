package logcompactor

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	clickhousecontainer "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

const clickHouseTestImage = "clickhouse/clickhouse-server:24.3-alpine"

func setupClickHouseIntegration(t *testing.T) (driver.Conn, func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("clickhouse integration test (run in make test-full / CI full-test)")
	}

	ctx := context.Background()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	initSQL := filepath.Join(filepath.Dir(filename), "..", "..", "deploy", "clickhouse", "init.sql")

	chContainer, err := clickhousecontainer.Run(ctx,
		clickHouseTestImage,
		clickhousecontainer.WithInitScripts(initSQL),
		clickhousecontainer.WithDatabase("ad_event_processor"),
	)
	require.NoError(t, err)

	dsn, err := chContainer.ConnectionString(ctx)
	require.NoError(t, err)

	conn := openClickHouseTestConn(t, dsn)
	cleanup := func() {
		_ = conn.Close()
		_ = chContainer.Terminate(ctx)
	}
	return conn, cleanup
}

func openClickHouseTestConn(t *testing.T, dsn string) driver.Conn {
	t.Helper()
	opts, err := chgo.ParseDSN(dsn)
	require.NoError(t, err)
	opts.Settings = chgo.Settings{
		"async_insert":          0,
		"wait_for_async_insert": 1,
	}
	opts.DialTimeout = 10 * time.Second

	conn, err := chgo.Open(opts)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return conn.Ping(context.Background()) == nil
	}, 30*time.Second, 500*time.Millisecond, "clickhouse ping")

	return conn
}

func queryCHExplainPlan(t *testing.T, conn driver.Conn, query string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx, "EXPLAIN indexes = 1\n"+query)
	require.NoError(t, err)
	defer rows.Close()

	var plan string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		if plan != "" {
			plan += "\n"
		}
		plan += line
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, plan)
	return plan
}

// summarizeExplainPlan flattens a ClickHouse EXPLAIN tree into a single log line.
func summarizeExplainPlan(plan string) string {
	var parts []string
	for _, line := range strings.Split(plan, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "ReadFromMergeTree"):
			parts = append(parts, "engine=MergeTree")
		case line == "PrimaryKey", line == "Partition", line == "MinMax":
			parts = append(parts, strings.ToLower(line))
		case strings.HasPrefix(line, "Parts:"):
			parts = append(parts, "parts="+strings.TrimSpace(strings.TrimPrefix(line, "Parts:")))
		case strings.HasPrefix(line, "Granules:"):
			parts = append(parts, "granules="+strings.TrimSpace(strings.TrimPrefix(line, "Granules:")))
		}
	}
	if len(parts) == 0 {
		return "plan_ok"
	}
	return strings.Join(parts, " ")
}

package database

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	clickhousecontainer "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

const chJanitorTestImage = "clickhouse/clickhouse-server:24.3-alpine"

func setupCHJanitorIntegration(t *testing.T) (driver.Conn, func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("clickhouse integration test (run in make test-full / CI full-test)")
	}

	ctx := context.Background()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	initSQL := filepath.Join(filepath.Dir(filename), "..", "..", "deploy", "clickhouse", "init.sql")

	chContainer, err := clickhousecontainer.Run(ctx,
		chJanitorTestImage,
		clickhousecontainer.WithInitScripts(initSQL),
		clickhousecontainer.WithDatabase("ad_event_processor"),
	)
	require.NoError(t, err)

	dsn, err := chContainer.ConnectionString(ctx)
	require.NoError(t, err)

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

	cleanup := func() {
		_ = conn.Close()
		_ = chContainer.Terminate(ctx)
	}
	return conn, cleanup
}

func insertImpressionPart(t *testing.T, conn driver.Conn, clickID string, createdAt time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, conn.Exec(ctx, `
INSERT INTO ad_event_processor.impressions
(click_id, campaign_id, ip_address, user_agent, payload, created_at)
VALUES (?, ?, '203.0.113.1', 'janitor-test', 'payload-bytes', ?)`,
		clickID, uuid.New(), createdAt,
	))
}

func countActiveParts(t *testing.T, conn driver.Conn, table, partition string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n uint64
	require.NoError(t, conn.QueryRow(ctx, `
SELECT count()
FROM system.parts
WHERE active AND database = currentDatabase() AND table = ? AND partition = ?`,
		table, partition).Scan(&n))
	return n
}

func TestCHPartitionJanitor_Recompress_RealCH(t *testing.T) {
	conn, cleanup := setupCHJanitorIntegration(t)
	defer cleanup()

	partitionTime := time.Date(2025, 3, 15, 12, 0, 0, 0, time.UTC)
	partition := partitionTime.Format("200601")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	require.NoError(t, conn.Exec(ctx, `SYSTEM STOP MERGES impressions`))
	t.Cleanup(func() { _ = conn.Exec(context.Background(), `SYSTEM START MERGES impressions`) })
	for i := 0; i < 10; i++ {
		insertImpressionPart(t, conn, fmt.Sprintf("recompress-%d", i), partitionTime)
	}
	require.GreaterOrEqual(t, countActiveParts(t, conn, "impressions", partition), uint64(8))
	require.NoError(t, conn.Exec(ctx, `SYSTEM START MERGES impressions`))

	offPeak := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	j := NewCHPartitionJanitor(conn, CHJanitorOptions{
		RecompressPartsThreshold: 8,
		OffPeakStartHourUTC:      2,
		OffPeakEndHourUTC:        6,
		Now:                      func() time.Time { return offPeak },
	})

	require.NoError(t, j.runRecompress(ctx))
	require.Less(t, countActiveParts(t, conn, "impressions", partition), uint64(8))
}

func TestCHPartitionJanitor_EmergencyDrop_RealCH(t *testing.T) {
	conn, cleanup := setupCHJanitorIntegration(t)
	defer cleanup()

	oldMonth := time.Now().UTC().AddDate(0, -2, 0)
	partition := oldMonth.Format("200601")
	insertImpressionPart(t, conn, "emergency-drop-"+uuid.NewString(), oldMonth)

	var alerted bool
	j := NewCHPartitionJanitor(conn, CHJanitorOptions{
		EmergencyDropPercent: 90,
		DiskUsedPercentFn: func(context.Context) (float64, error) {
			return 95.0, nil
		},
		OnEmergencyDrop: func(table, part string, diskPct float64) {
			alerted = true
			require.Equal(t, "impressions", table)
			require.Equal(t, partition, part)
			require.InDelta(t, 95.0, diskPct, 0.01)
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, j.runEmergencyDrop(ctx, 95.0))
	require.True(t, alerted)

	var rows uint64
	require.NoError(t, conn.QueryRow(ctx, `
SELECT count() FROM ad_event_processor.impressions WHERE toYYYYMM(created_at) = ?`, partition).Scan(&rows))
	require.Equal(t, uint64(0), rows)
}

func TestCHPartitionJanitor_RetentionDrop_RealCH(t *testing.T) {
	conn, cleanup := setupCHJanitorIntegration(t)
	defer cleanup()

	oldMonth := time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)
	insertImpressionPart(t, conn, "retention-"+uuid.NewString(), oldMonth)

	j := NewCHPartitionJanitor(conn, CHJanitorOptions{RetentionDays: 180})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, j.runRetentionDrop(ctx))

	var rows uint64
	require.NoError(t, conn.QueryRow(ctx, `
SELECT count() FROM ad_event_processor.impressions WHERE toYYYYMM(created_at) = '202001'`).Scan(&rows))
	require.Equal(t, uint64(0), rows)
}

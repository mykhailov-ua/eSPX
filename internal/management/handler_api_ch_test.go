package management

import (
	"bytes"
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"espx/internal/clickhouse/migrate"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	clickhousecontainer "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

const clickHouseStatsTestImage = "clickhouse/clickhouse-server:24.3-alpine"

func setupClickHouseStatsTest(t *testing.T) (driver.Conn, func()) {
	t.Helper()
	conn, _, cleanup := setupClickHouseStatsContainer(t)
	return conn, cleanup
}

func setupClickHouseStatsContainer(t *testing.T) (driver.Conn, testcontainers.Container, func()) {
	t.Helper()
	ctx := context.Background()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	initSQL := filepath.Join(filepath.Dir(filename), "..", "..", "deploy", "clickhouse", "init.sql")

	chContainer, err := clickhousecontainer.Run(ctx,
		clickHouseStatsTestImage,
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
	}, 30*time.Second, 500*time.Millisecond)

	cleanup := func() {
		_ = conn.Close()
		_ = chContainer.Terminate(ctx)
	}
	return conn, chContainer, cleanup
}

func TestHandlerAPI_CampaignStatsClickHouseExplain(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	conn, cleanup := setupClickHouseStatsTest(t)
	defer cleanup()
	ctx := context.Background()
	require.NoError(t, migrate.ApplyClickHouseMigrations(ctx, conn))

	campaignID := uuid.MustParse("00000000-0000-4000-8000-000000000042")
	hour := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Hour)
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO impressions (click_id, campaign_id, ip_address, user_agent, payload, created_at)
		VALUES (?, ?, '1.1.1.1', 'ua', '{}', ?)`,
		"explain-click-1", campaignID, hour.Add(5*time.Minute)))

	from := hour.Add(-time.Hour)
	to := hour.Add(2 * time.Hour)
	query := `
EXPLAIN indexes = 1
SELECT
    hour,
    sum(impressions) AS impressions,
    sum(clicks) AS clicks,
    sum(conversions) AS conversions
FROM (
    SELECT hour, impression_count AS impressions, toUInt64(0) AS clicks, toUInt64(0) AS conversions
    FROM mv_campaign_hourly_impressions
    WHERE campaign_id = ? AND hour >= ? AND hour < ?
    UNION ALL
    SELECT hour, toUInt64(0), click_count, toUInt64(0)
    FROM mv_campaign_hourly_clicks
    WHERE campaign_id = ? AND hour >= ? AND hour < ?
    UNION ALL
    SELECT hour, toUInt64(0), toUInt64(0), conversion_count
    FROM mv_campaign_hourly_conversions
    WHERE campaign_id = ? AND hour >= ? AND hour < ?
)
GROUP BY hour
ORDER BY hour`

	rows, err := conn.Query(ctx, query,
		campaignID, from, to,
		campaignID, from, to,
		campaignID, from, to,
	)
	require.NoError(t, err)
	defer rows.Close()

	var plan bytes.Buffer
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	t.Logf("ClickHouse EXPLAIN:\n%s", plan.String())
	assert.Contains(t, plan.String(), "ReadFromMergeTree")
	assert.Contains(t, plan.String(), "PrimaryKey")
}

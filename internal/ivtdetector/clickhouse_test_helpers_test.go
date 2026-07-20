package ivtdetector

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

const clickHouseTestImage = "clickhouse/clickhouse-server:24.3-alpine"

func setupClickHouseTest(t *testing.T) (driver.Conn, func()) {
	t.Helper()
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

func seedClickHouseEvents(t *testing.T, conn driver.Conn, ip, userAgent string, impressions, clicks int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	campaignID := uuid.New()
	now := time.Now().UTC()

	for i := 0; i < impressions; i++ {
		clickID := fmt.Sprintf("imp-%s-%d", ip, i)
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO ad_event_processor.impressions
			(click_id, campaign_id, ip_address, user_agent, payload, created_at)
			VALUES (?, ?, ?, ?, '', ?)`,
			clickID, campaignID, ip, userAgent, now,
		))
	}
	for i := 0; i < clicks; i++ {
		clickID := fmt.Sprintf("clk-%s-%d", ip, i)
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO ad_event_processor.clicks
			(click_id, campaign_id, ip_address, user_agent, payload, created_at)
			VALUES (?, ?, ?, ?, '', ?)`,
			clickID, campaignID, ip, userAgent, now,
		))
	}
}

func seedIntervalBotClicks(t *testing.T, conn driver.Conn, ip, userAgent string, count int, interval time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	campaignID := uuid.New()
	base := time.Now().UTC().Add(-interval * time.Duration(count))

	for i := 0; i < count; i++ {
		clickID := fmt.Sprintf("interval-%s-%d", ip, i)
		ts := base.Add(interval * time.Duration(i))
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO ad_event_processor.clicks
			(click_id, campaign_id, ip_address, user_agent, payload, created_at)
			VALUES (?, ?, ?, ?, '', ?)`,
			clickID, campaignID, ip, userAgent, ts,
		))
	}
}

func seedJitteredClicks(t *testing.T, conn driver.Conn, ip, userAgent string, deltas []time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	campaignID := uuid.New()
	ts := time.Now().UTC().Add(-time.Hour)
	for i, delta := range deltas {
		clickID := fmt.Sprintf("jitter-%s-%d", ip, i)
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO ad_event_processor.clicks
			(click_id, campaign_id, ip_address, user_agent, payload, created_at)
			VALUES (?, ?, ?, ?, '', ?)`,
			clickID, campaignID, ip, userAgent, ts,
		))
		ts = ts.Add(delta)
	}
}

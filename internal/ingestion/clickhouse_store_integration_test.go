package ingestion

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"espx/internal/campaignmodel"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	clickhousecontainer "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

const clickHouseTestImage = "clickhouse/clickhouse-server:24.3-alpine"

// setupClickHouseIntegration boots ClickHouse with production init.sql schema.
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

// openClickHouseTestConn dials ClickHouse with synchronous inserts for deterministic row counts.
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

func countClickHouseRows(t *testing.T, conn driver.Conn, table, clickID string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n uint64
	query := fmt.Sprintf("SELECT count() FROM ad_event_processor.%s WHERE click_id = ?", table)
	require.NoError(t, conn.QueryRow(ctx, query, clickID).Scan(&n))
	return n
}

// TestClickHouseStore_InsertDeduplicate_RealCH verifies insert_deduplicate=1 collapses retries to one row.
func TestClickHouseStore_InsertDeduplicate_RealCH(t *testing.T) {
	conn, cleanup := setupClickHouseIntegration(t)
	defer cleanup()

	store := NewClickHouseStore(conn, 5*time.Second, "", DefaultCHSpoolConfig(), nil)
	defer func() { _ = store.Close() }()

	clickID := "ch-dedup-" + uuid.NewString()
	campID := uuid.New()
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	evt := &campaignmodel.Event{
		ClickID:    clickID,
		CampaignID: campID,
		Type:       "impression",
		IP:         "203.0.113.10",
		UA:         "chaos-ch-dedup",
		Payload:    []byte(`{"chaos":"dedup"}`),
		CreatedAt:  createdAt,
	}

	const token = "chaos-ch-dedup-token-explicit"
	ctx := context.WithValue(context.Background(), campaignmodel.DeduplicationTokenKey, token)

	require.NoError(t, store.StoreBatch(ctx, []*campaignmodel.Event{evt}))
	require.NoError(t, store.StoreBatch(ctx, []*campaignmodel.Event{evt}))

	require.Equal(t, uint64(1), countClickHouseRows(t, conn, "impressions", clickID),
		"duplicate insert with same dedup token must yield one row")
}

// TestClickHouseStore_InsertDeduplicate_DeterministicToken_RealCH verifies processor-style SHA token dedupes retries.
func TestClickHouseStore_InsertDeduplicate_DeterministicToken_RealCH(t *testing.T) {
	conn, cleanup := setupClickHouseIntegration(t)
	defer cleanup()

	store := NewClickHouseStore(conn, 5*time.Second, "", DefaultCHSpoolConfig(), nil)
	defer func() { _ = store.Close() }()

	clickID := "ch-dedup-det-" + uuid.NewString()
	campID := uuid.New()
	createdAt := time.Unix(1_700_000_100, 0).UTC()
	evt := &campaignmodel.Event{
		ClickID:    clickID,
		CampaignID: campID,
		Type:       "click",
		IP:         "203.0.113.11",
		UA:         "chaos-ch-dedup",
		Payload:    []byte(`{"chaos":"dedup-det"}`),
		CreatedAt:  createdAt,
	}

	ctx := context.Background()
	require.NoError(t, store.StoreBatch(ctx, []*campaignmodel.Event{evt}))
	require.NoError(t, store.StoreBatch(ctx, []*campaignmodel.Event{evt}))

	require.Equal(t, uint64(1), countClickHouseRows(t, conn, "clicks", clickID),
		"deterministic dedup token from event batch must collapse duplicate inserts")
}

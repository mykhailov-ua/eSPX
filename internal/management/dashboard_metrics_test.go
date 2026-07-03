package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestDashboardMetricsCollector_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx := context.Background()
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), nil)
	defer svc.Close()

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, custID, "Dash Metrics", 100_000_000, "USD"))

	spec := testCampaignSpec(custID, "Active", 50_000_000, "dash-metrics-active")
	_, err := svc.CreateCampaign(ctx, spec)
	require.NoError(t, err)

	_, err = db.New(pool).CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: "UPDATE_CAMPAIGN",
		Payload:   []byte(`{"id":"test"}`),
	})
	require.NoError(t, err)

	q := db.New(pool)
	expectedActive, err := q.CountCampaigns(ctx, db.CountCampaignsParams{
		Status: pgtype.Text{String: "ACTIVE", Valid: true},
	})
	require.NoError(t, err)
	expectedPending, err := q.CountPendingOutboxEvents(ctx)
	require.NoError(t, err)

	collector := NewDashboardMetricsCollector(svc, nil, &config.Config{
		RedisStreamName: "dash:test:stream",
		FraudStreamName: "dash:test:fraud",
	})

	now := time.Now().UTC()
	snap, err := collector.Collect(ctx, now)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", expectedActive), snap.ActiveCampaigns)
	require.Equal(t, fmt.Sprintf("%d", expectedPending), snap.PendingOutbox)
	require.NotEmpty(t, snap.RequestsPerSec)
	require.NotEmpty(t, snap.TrafficChart.Series)
}

func TestDashboardMetricsCollector_RedisIngestRate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx := context.Background()
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), nil)
	defer svc.Close()

	stream := "dash:rate:test"
	cfg := &config.Config{RedisStreamName: stream, FraudStreamName: stream + ":fraud"}

	collector := NewDashboardMetricsCollector(svc, nil, cfg)

	now := time.Now().UTC()
	_, err := collector.Collect(ctx, now)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"n": i},
		}).Err())
	}

	time.Sleep(10 * time.Millisecond)
	snap, err := collector.Collect(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.NotEqual(t, "0", snap.RequestsPerSec)
}

func TestDashboardMetricsCollector_AuditRows(t *testing.T) {
	rows := []db.AdminAuditLog{
		{
			Action:     "UPDATE_CAMPAIGN_PACING",
			TargetType: "campaign",
			TargetID:   pgtype.UUID{Bytes: uuid.New(), Valid: true},
			CreatedAt:  pgtype.Timestamptz{Time: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), Valid: true},
		},
	}
	events := auditRowsToEvents(rows, time.Now().UTC())
	require.Len(t, events, 1)
	require.Equal(t, "campaign", events[0].Type)
	require.Contains(t, events[0].Detail, "UPDATE_CAMPAIGN_PACING")
}

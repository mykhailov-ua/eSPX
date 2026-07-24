// broker_ingest_test.go exercises broker produce → live consumer → Postgres settlement (M6-19).
package e2e_test

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/ingestion"
	"espx/internal/ingestion/pb"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/testutil"
	"espx/pkg/broker/client"
	bserver "espx/pkg/broker/server"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_BrokerIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping broker e2e")
	}

	pool, cleanupDB := testutil.SetupAdsPostgres(t)
	defer cleanupDB()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := db.New(pool)
	partManager := database.NewPartitionManager(pool, 7, 2)
	require.NoError(t, partManager.Run(ctx))

	customerID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)", customerID, "Broker Customer", 1_000_000_000)
	require.NoError(t, err)

	campaignID := uuid.New()
	_, err = pool.Exec(ctx, "INSERT INTO campaigns (id, name, status, customer_id, budget_limit) VALUES ($1, $2, $3, $4, $5)", campaignID, "Broker Campaign", "ACTIVE", customerID, 100_000_000)
	require.NoError(t, err)

	srv := bserver.NewServer("127.0.0.1:0", t.TempDir(), 8*1024*1024, 4096)
	require.NoError(t, srv.Start())
	defer srv.Stop()

	producer := client.NewClient(srv.Addr(), 2*time.Second)
	require.NoError(t, producer.Connect())
	rec := &pb.AdStreamEvent{
		CreatedAtUnix: time.Now().Unix(),
		CampaignId:    campaignID[:],
		ClickId:       []byte("broker-e2e-click"),
		EventType:     []byte("click"),
		Ip:            []byte("203.0.113.9"),
		UserId:        []byte("broker-user"),
	}
	data := make([]byte, rec.SizeVT())
	n, err := rec.MarshalToSizedBufferVT(data)
	require.NoError(t, err)
	_, err = producer.Produce("tracker-logs", 0, data[:n])
	require.NoError(t, err)
	require.NoError(t, producer.Close())

	store := ingestion.NewPostgresStore(queries, 200*time.Millisecond)
	cfg := ingestion.BrokerConsumerConfig{
		BrokerAddr: srv.Addr(),
		Topic:      "tracker-logs",
		Group:      "e2e-broker",
		BatchSize:  1,
		FlushInt:   50 * time.Millisecond,
		MaxBytes:   1024 * 1024,
		Timeout:    2 * time.Second,
		IdleWait:   20 * time.Millisecond,
		ShadowMode: false,
	}
	consumer := ingestion.NewBrokerStreamConsumer(store, cfg, time.Second, 50*time.Millisecond, time.Second, 1)
	consumer.Start(ctx)
	defer consumer.Close()

	assert.Eventually(t, func() bool {
		var clicks int64
		err := pool.QueryRow(ctx, "SELECT clicks_count FROM campaign_stats WHERE campaign_id = $1", campaignID).Scan(&clicks)
		return err == nil && clicks == 1
	}, 8*time.Second, 100*time.Millisecond)

	assert.Eventually(t, func() bool {
		var eventCount int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE campaign_id = $1", campaignID).Scan(&eventCount)
		return err == nil && eventCount == 1
	}, 8*time.Second, 100*time.Millisecond)
}

package ingestion

import (
	"context"
	"testing"
	"time"

	"espx/internal/metrics"
	"espx/pkg/broker/client"
	bserver "espx/pkg/broker/server"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBrokerReconcileWorker_Divergence(t *testing.T) {
	srv := bserver.NewServer("127.0.0.1:0", t.TempDir(), 1024*1024, 4096)
	require.NoError(t, srv.Start())
	defer srv.Stop()

	producer := client.NewClient(srv.Addr(), 2*time.Second)
	require.NoError(t, producer.Connect())
	_, err := producer.Produce("tracker-logs", 0, []byte("x"))
	require.NoError(t, err)
	_, err = producer.Produce("tracker-logs", 0, []byte("y"))
	require.NoError(t, err)
	require.NoError(t, producer.Close())

	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	ctx := context.Background()
	stream := "events:shard0"
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"payload": "a"},
	}).Err())
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"payload": "b"},
	}).Err())
	require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"payload": "c"},
	}).Err())

	w := NewBrokerReconcileWorker(BrokerReconcileConfig{
		BrokerAddr:          srv.Addr(),
		Topic:               "tracker-logs",
		PartitionCount:      1,
		BrokerGroup:         "recon-group",
		StreamName:          stream,
		Interval:            time.Hour,
		DivergenceThreshold: 1,
	}, []redis.UniversalClient{rdb})
	require.NoError(t, w.cli.Connect())
	w.sample(ctx)

	div := testutil.ToFloat64(metrics.BrokerIngestDivergenceMessages.WithLabelValues("tracker-logs", "recon-group"))
	assert.Greater(t, div, float64(0))
	high := testutil.ToFloat64(metrics.BrokerIngestDivergenceHigh.WithLabelValues("tracker-logs", "recon-group"))
	assert.Equal(t, float64(1), high)
}

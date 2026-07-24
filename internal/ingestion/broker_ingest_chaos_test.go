package ingestion

import (
	"context"
	"fmt"
	"testing"
	"time"

	"espx/internal/ingestion/pb"
	"espx/internal/metrics"
	"espx/pkg/broker/client"
	bserver "espx/pkg/broker/server"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func startBrokerChaosServer(t *testing.T) (*bserver.Server, string) {
	t.Helper()
	srv := bserver.NewServer("127.0.0.1:0", t.TempDir(), 1024*1024, 4096)
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)
	return srv, srv.Addr()
}

// TestChaos_BrokerLiveConsumer_CorruptPayload advances offset past poison vtproto (M6-12 chaos proof).
func TestChaos_BrokerLiveConsumer_CorruptPayload(t *testing.T) {
	_, addr := startBrokerChaosServer(t)

	producer := client.NewClient(addr, 2*time.Second)
	require.NoError(t, producer.Connect())
	_, err := producer.Produce("tracker-logs", 0, []byte{0xff, 0xfe, 0x01, 0x02})
	require.NoError(t, err)
	campID := uuid.New()
	produceBrokerStreamEvent(t, producer, "tracker-logs", &pb.AdStreamEvent{
		CreatedAtUnix: time.Now().Unix(),
		CampaignId:    campID[:],
		ClickId:       []byte("chaos-good-click"),
		EventType:     []byte("click"),
	})
	require.NoError(t, producer.Close())

	parseBefore := testutil.ToFloat64(metrics.BrokerIngestParseErrorsTotal.WithLabelValues("tracker-logs", "chaos-corrupt"))
	store := &MockEventStore{}
	cfg := BrokerConsumerConfig{
		BrokerAddr: addr,
		Topic:      "tracker-logs",
		Group:      "chaos-corrupt",
		BatchSize:  1,
		FlushInt:   50 * time.Millisecond,
		MaxBytes:   1024 * 1024,
		Timeout:    2 * time.Second,
		IdleWait:   20 * time.Millisecond,
		ShadowMode: false,
	}
	consumer := NewBrokerStreamConsumer(store, cfg, time.Second, 50*time.Millisecond, time.Second, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	consumer.Start(ctx)
	defer consumer.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		flushes := len(store.flushes)
		store.mu.Unlock()
		if flushes >= 1 {
			parseAfter := testutil.ToFloat64(metrics.BrokerIngestParseErrorsTotal.WithLabelValues("tracker-logs", "chaos-corrupt"))
			require.GreaterOrEqual(t, parseAfter, parseBefore+1)

			check := client.NewClient(addr, 2*time.Second)
			require.NoError(t, check.Connect())
			off, err := check.CommittedOffset("tracker-logs", 0, "chaos-corrupt")
			_ = check.Close()
			require.NoError(t, err)
			require.GreaterOrEqual(t, off, uint64(2))

			logChaosProof(t, "broker_corrupt_payload_skip", map[string]string{
				"flushes":      fmt.Sprintf("%d", flushes),
				"parse_errors": fmt.Sprintf("%.0f", parseAfter-parseBefore),
				"offset":       fmt.Sprintf("%d", off),
			})
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("broker consumer did not skip corrupt payload and flush valid event")
}

// TestChaos_BrokerLiveConsumer_ReconnectOffsetResume verifies a new consumer resumes committed offset.
func TestChaos_BrokerLiveConsumer_ReconnectOffsetResume(t *testing.T) {
	_, addr := startBrokerChaosServer(t)
	topic := "tracker-logs"
	group := "chaos-reconnect"
	campID := uuid.New()

	producer := client.NewClient(addr, 2*time.Second)
	require.NoError(t, producer.Connect())
	produceBrokerStreamEvent(t, producer, topic, &pb.AdStreamEvent{
		CreatedAtUnix: time.Now().Unix(),
		CampaignId:    campID[:],
		ClickId:       []byte("reconnect-click-1"),
		EventType:     []byte("click"),
	})
	require.NoError(t, producer.Close())

	store1 := &MockEventStore{}
	cfg := BrokerConsumerConfig{
		BrokerAddr: addr,
		Topic:      topic,
		Group:      group,
		BatchSize:  1,
		FlushInt:   50 * time.Millisecond,
		MaxBytes:   1024 * 1024,
		Timeout:    2 * time.Second,
		IdleWait:   20 * time.Millisecond,
		ShadowMode: false,
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	consumer1 := NewBrokerStreamConsumer(store1, cfg, time.Second, 50*time.Millisecond, time.Second, 1)
	consumer1.Start(ctx1)
	waitBrokerConsumerFlush(t, store1, 1)
	consumer1.Close()
	cancel1()
	_ = consumer1.Wait(context.Background())

	producer2 := client.NewClient(addr, 2*time.Second)
	require.NoError(t, producer2.Connect())
	produceBrokerStreamEvent(t, producer2, topic, &pb.AdStreamEvent{
		CreatedAtUnix: time.Now().Unix(),
		CampaignId:    campID[:],
		ClickId:       []byte("reconnect-click-2"),
		EventType:     []byte("click"),
	})
	require.NoError(t, producer2.Close())

	store2 := &MockEventStore{}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	consumer2 := NewBrokerStreamConsumer(store2, cfg, time.Second, 50*time.Millisecond, time.Second, 1)
	consumer2.Start(ctx2)
	defer consumer2.Close()
	waitBrokerConsumerFlush(t, store2, 1)

	check := client.NewClient(addr, 2*time.Second)
	require.NoError(t, check.Connect())
	off, err := check.CommittedOffset(topic, 0, group)
	_ = check.Close()
	require.NoError(t, err)
	require.GreaterOrEqual(t, off, uint64(2))

	logChaosProof(t, "broker_reconnect_offset_resume", map[string]string{
		"first_flushes":  "1",
		"second_flushes": "1",
		"offset":         fmt.Sprintf("%d", off),
	})
}

func waitBrokerConsumerFlush(t *testing.T, store *MockEventStore, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		n := len(store.flushes)
		store.mu.Unlock()
		if n >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("broker consumer did not flush %d batches", want)
}

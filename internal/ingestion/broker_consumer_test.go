package ingestion

import (
	"context"
	"testing"
	"time"

	"espx/internal/ingestion/pb"
	"espx/pkg/broker/client"
	bserver "espx/pkg/broker/server"

	"github.com/google/uuid"
)

func TestBrokerStreamConsumer_ShadowMode(t *testing.T) {
	srv := bserver.NewServer("127.0.0.1:0", t.TempDir(), 1024*1024, 4096)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	producer := client.NewClient(srv.Addr(), 2*time.Second)
	if err := producer.Connect(); err != nil {
		t.Fatal(err)
	}
	campID := uuid.New()
	rec := &pb.AdStreamEvent{
		CreatedAtUnix: time.Now().Unix(),
		CampaignId:    campID[:],
		ClickId:       []byte("broker-click"),
		EventType:     []byte("click"),
		Ip:            []byte("203.0.113.1"),
		UserId:        []byte("user-1"),
	}
	data := make([]byte, rec.SizeVT())
	n, err := rec.MarshalToSizedBufferVT(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := producer.Produce("tracker-logs", 0, data[:n]); err != nil {
		t.Fatal(err)
	}
	_ = producer.Close()

	store := &MockEventStore{}
	cfg := BrokerConsumerConfig{
		BrokerAddr: srv.Addr(),
		Topic:      "tracker-logs",
		Group:      "test-shadow",
		BatchSize:  1,
		FlushInt:   50 * time.Millisecond,
		MaxBytes:   1024 * 1024,
		Timeout:    2 * time.Second,
		IdleWait:   20 * time.Millisecond,
		ShadowMode: true,
	}
	consumer := NewBrokerStreamConsumer(store, cfg, time.Second, 50*time.Millisecond, time.Second, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	consumer.Start(ctx)
	defer consumer.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		n := len(store.flushes)
		store.mu.Unlock()
		if n > 0 {
			t.Fatal("shadow mode must not write to store")
		}
		check := client.NewClient(srv.Addr(), 2*time.Second)
		if err := check.Connect(); err != nil {
			t.Fatal(err)
		}
		off, err := check.CommittedOffset("tracker-logs", 0, "test-shadow")
		_ = check.Close()
		if err == nil && off > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("broker shadow consumer did not commit offset")
}

func produceBrokerStreamEvent(t *testing.T, producer *client.Client, topic string, rec *pb.AdStreamEvent) {
	t.Helper()
	data := make([]byte, rec.SizeVT())
	n, err := rec.MarshalToSizedBufferVT(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := producer.Produce(topic, 0, data[:n]); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerStreamConsumer_LiveFlush(t *testing.T) {
	srv := bserver.NewServer("127.0.0.1:0", t.TempDir(), 1024*1024, 4096)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	producer := client.NewClient(srv.Addr(), 2*time.Second)
	if err := producer.Connect(); err != nil {
		t.Fatal(err)
	}
	campID := uuid.New()
	produceBrokerStreamEvent(t, producer, "tracker-logs", &pb.AdStreamEvent{
		CreatedAtUnix: time.Now().Unix(),
		CampaignId:    campID[:],
		ClickId:       []byte("live-click"),
		EventType:     []byte("click"),
		Ip:            []byte("203.0.113.1"),
		UserId:        []byte("user-1"),
	})
	_ = producer.Close()

	store := &MockEventStore{}
	cfg := BrokerConsumerConfig{
		BrokerAddr: srv.Addr(),
		Topic:      "tracker-logs",
		Group:      "test-live",
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
			check := client.NewClient(srv.Addr(), 2*time.Second)
			if err := check.Connect(); err != nil {
				t.Fatal(err)
			}
			off, err := check.CommittedOffset("tracker-logs", 0, "test-live")
			_ = check.Close()
			if err == nil && off > 0 {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("broker live consumer did not flush store and commit offset")
}

// TestBrokerStreamConsumer_CorruptPayload skips bad vtproto and advances offset (M6-12).
func TestBrokerStreamConsumer_CorruptPayload(t *testing.T) {
	srv := bserver.NewServer("127.0.0.1:0", t.TempDir(), 1024*1024, 4096)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	producer := client.NewClient(srv.Addr(), 2*time.Second)
	if err := producer.Connect(); err != nil {
		t.Fatal(err)
	}
	if _, err := producer.Produce("tracker-logs", 0, []byte{0xff, 0xfe, 0x01}); err != nil {
		t.Fatal(err)
	}
	campID := uuid.New()
	produceBrokerStreamEvent(t, producer, "tracker-logs", &pb.AdStreamEvent{
		CreatedAtUnix: time.Now().Unix(),
		CampaignId:    campID[:],
		ClickId:       []byte("good-click"),
		EventType:     []byte("click"),
	})
	_ = producer.Close()

	store := &MockEventStore{}
	cfg := BrokerConsumerConfig{
		BrokerAddr: srv.Addr(),
		Topic:      "tracker-logs",
		Group:      "test-corrupt",
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
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("consumer did not skip corrupt payload and flush valid event")
}

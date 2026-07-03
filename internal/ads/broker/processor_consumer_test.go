package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"espx/internal/ads/processor"
	adstest "espx/internal/ads/testutil"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Guards stream consumer ingests valid events into the event store.
func TestStreamConsumer_Ingestion(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	producer := NewStreamProducer(rdb, "s1", 1000, 1*time.Second)
	err := producer.Process(&domain.Event{CampaignID: uuid.New(), Type: "click", ClickID: uuid.NewString()})
	assert.NoError(t, err)
}

// Guards batch flush commits events and acknowledges stream IDs together.
func TestStreamConsumer_BatchFlushing(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	mockStore := &adstest.MockEventStore{}
	producer := NewStreamProducer(rdb, "s2", 1000, 1*time.Second)
	proc := processor.NewStreamConsumer(mockStore, rdb, "s2", "g2", "c2", 2, 1, 10*time.Second, 1*time.Second, 10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Minute, 1*time.Second)

	proc.Start(context.Background())
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 3; i++ {
		_ = producer.Process(&domain.Event{CampaignID: uuid.New(), Type: "click", ClickID: uuid.NewString()})
	}

	time.Sleep(200 * time.Millisecond)
	proc.Close()
	proc.Wait(context.Background())

	assert.GreaterOrEqual(t, mockStore.FlushCount(), 1)
}

// Guards unprocessable stream entries land in DLQ after retry exhaustion.
func TestStreamConsumer_DLQ(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	failStore := &failingEventStore{
		failErr: errors.New("simulated poison pill"),
	}

	producer := NewStreamProducer(rdb, "s_dlq", 1000, 1*time.Second)
	proc := processor.NewStreamConsumer(failStore, rdb, "s_dlq", "g_dlq", "c_dlq", 2, 1, 10*time.Millisecond, 1*time.Second, 10*time.Millisecond, 10*time.Millisecond, 1, 1*time.Minute, 1*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proc.Start(ctx)

	for i := 0; i < 2; i++ {
		_ = producer.Process(&domain.Event{CampaignID: uuid.New(), Type: "click", ClickID: uuid.NewString()})
	}

	assert.Eventually(t, func() bool {
		size, err := rdb.XLen(ctx, "ad:events:dlq").Result()
		return err == nil && size == 2
	}, 3*time.Second, 50*time.Millisecond, "Should have 2 events in DLQ")

	pending, err := rdb.XPending(ctx, "s_dlq", "g_dlq").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)

	proc.Close()
	proc.Wait(ctx)
}

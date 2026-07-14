package mlanalytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMicroBatch_AggregationAndScoring(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx := context.Background()
	scorer, err := NewLGBMScorer("testdata/model.txt")
	require.NoError(t, err)

	mb := NewMicroBatcher(rdb, scorer)
	go mb.Start(ctx)

	campaignID := uuid.New()

	// Enqueue 10 events (with high click count to trigger high score boost)
	now := time.Now()
	for i := 0; i < 10; i++ {
		evt := &domain.Event{
			IP:         "1.2.3.4",
			CampaignID: campaignID,
			Type:       "click",
			UserID:     "user1",
			UA:         "ua1",
			CreatedAt:  now,
		}
		// Message ID format: <timestamp>-<sequence>
		msgID := fmt.Sprintf("%d-0", now.UnixNano()/1e6)
		mb.Enqueue(evt, msgID)
	}

	// Wait for the 100 ms micro-batch window to trigger flush
	time.Sleep(250 * time.Millisecond)

	// Verify that the score boost key was written to Redis
	key := fmt.Sprintf("ml:score:boost:%s", campaignID.String())
	val, err := rdb.Get(ctx, key).Result()
	require.NoError(t, err)

	// Since clicks are high, the score boost should be set
	assert.NotEmpty(t, val)
	ttl, err := rdb.TTL(ctx, key).Result()
	require.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 30*time.Second)
}

func TestMicroBatch_StreamLagPause(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"}) // Mock client, won't be used
	defer rdb.Close()

	mb := NewMicroBatcher(rdb, nil)

	campaignID := uuid.New()
	evt := &domain.Event{
		IP:         "1.2.3.4",
		CampaignID: campaignID,
		Type:       "click",
		CreatedAt:  time.Now(),
	}

	// Enqueue with a message ID that has > 30 seconds lag (e.g. 40 seconds ago)
	staleTime := time.Now().Add(-40 * time.Second)
	msgID := fmt.Sprintf("%d-0", staleTime.UnixNano()/1e6)

	mb.Enqueue(evt, msgID)

	// The event should be dropped, so the channel should remain empty
	assert.Len(t, mb.eventsChan, 0)
}

func TestMicroBatch_BoundedQueueDrop(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"}) // Mock client
	defer rdb.Close()

	mb := NewMicroBatcher(rdb, nil)

	// Fill the channel to capacity (10000)
	campaignID := uuid.New()
	for i := 0; i < 10000; i++ {
		evt := &domain.Event{
			IP:         "1.2.3.4",
			CampaignID: campaignID,
			Type:       "click",
			CreatedAt:  time.Now(),
		}
		msgID := fmt.Sprintf("%d-0", time.Now().UnixNano()/1e6)
		mb.Enqueue(evt, msgID)
	}

	assert.Len(t, mb.eventsChan, 10000)

	// Enqueuing one more event should drop it and not block
	evt := &domain.Event{
		IP:         "1.2.3.4",
		CampaignID: campaignID,
		Type:       "click",
		CreatedAt:  time.Now(),
	}
	msgID := fmt.Sprintf("%d-0", time.Now().UnixNano()/1e6)

	done := make(chan bool, 1)
	go func() {
		mb.Enqueue(evt, msgID)
		done <- true
	}()

	select {
	case <-done:
		// Success: Enqueue did not block
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Enqueue blocked on full channel")
	}
}

func TestChaos_MLProcessorLag(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"}) // Mock client
	defer rdb.Close()

	mb := NewMicroBatcher(rdb, nil)

	campaignID := uuid.New()
	evt := &domain.Event{
		IP:         "1.2.3.4",
		CampaignID: campaignID,
		Type:       "click",
		CreatedAt:  time.Now(),
	}

	// 1. Enqueue with no lag -> should succeed
	msgIDNormal := fmt.Sprintf("%d-0", time.Now().UnixNano()/1e6)
	mb.Enqueue(evt, msgIDNormal)
	assert.Len(t, mb.eventsChan, 1)

	// 2. Enqueue with high lag (> 30 s) -> should pause/drop
	staleTime := time.Now().Add(-40 * time.Second)
	msgIDStale := fmt.Sprintf("%d-0", staleTime.UnixNano()/1e6)
	mb.Enqueue(evt, msgIDStale)

	// The second event should be dropped, so channel length remains 1
	assert.Len(t, mb.eventsChan, 1)

	logChaosProof(t, "ml_processor_lag", map[string]string{
		"subsystem": "ml_analytics",
		"lag_sec":   "40.0",
		"paused":    "true",
		"status":    "success",
	})
}

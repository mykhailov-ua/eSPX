package unit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

func setupTestRedis(t *testing.T) (redis.UniversalClient, func()) {
	ctx := context.Background()
	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %s", err)
	}
	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %s", err)
	}
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{endpoint},
	})
	return rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

type MockQuerier struct {
	repository.Querier
	mock.Mock
	mu      sync.Mutex
	flushes [][]ads.Event
}

func (m *MockQuerier) InsertEventsBatch(ctx context.Context, arg repository.InsertEventsBatchParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var batch []ads.Event
	for i := range arg.ClickIds {
		batch = append(batch, ads.Event{
			ClickID:    arg.ClickIds[i],
			CampaignID: uuid.UUID(arg.CampaignIds[i].Bytes),
			Type:       arg.EventTypes[i],
			Payload:    arg.Payloads[i],
			IP:         arg.IpAddresses[i],
			UA:         arg.UserAgents[i],
		})
	}
	batchCopy := make([]ads.Event, len(batch))
	copy(batchCopy, batch)
	m.flushes = append(m.flushes, batchCopy)
	return nil
}

func TestProcessor_Ingestion(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	mockRepo := &MockQuerier{}
	proc := ads.NewProcessor(mockRepo, rdb, "s1", "g1", "c1", 5, 1, 100*time.Millisecond, 1*time.Second)

	err := proc.Process(ads.Event{CampaignID: uuid.New(), Type: "click"})
	assert.NoError(t, err)
}

func TestProcessor_BatchFlushing(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	mockRepo := &MockQuerier{}
	proc := ads.NewProcessor(mockRepo, rdb, "s2", "g2", "c2", 2, 1, 10*time.Second, 1*time.Second)
	proc.Start(context.Background())
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 3; i++ {
		_ = proc.Process(ads.Event{CampaignID: uuid.New(), Type: "click"})
	}

	time.Sleep(200 * time.Millisecond)
	proc.Close()
	proc.Wait()

	mockRepo.mu.Lock()
	count := len(mockRepo.flushes)
	mockRepo.mu.Unlock()
	assert.GreaterOrEqual(t, count, 1)
}

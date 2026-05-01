package unit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/stretchr/testify/assert"
)

type MockTimerQuerier struct {
	repository.Querier
	mu      sync.Mutex
	flushed bool
}

func (m *MockTimerQuerier) InsertEventsBatch(ctx context.Context, arg repository.InsertEventsBatchParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushed = true
	return nil
}

func TestProcessor_FlushByTicker(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	mockRepo := new(MockTimerQuerier)
	proc := ads.NewProcessor(mockRepo, rdb, "st", "gt", "ct", 100, 1, 50*time.Millisecond, 1*time.Second)
	proc.Start(context.Background())
	time.Sleep(100 * time.Millisecond)
	defer proc.Close()

	_ = proc.Process(ads.Event{CampaignID: uuid.New(), Type: "click"})

	assert.Eventually(t, func() bool {
		mockRepo.mu.Lock()
		defer mockRepo.mu.Unlock()
		return mockRepo.flushed
	}, 1*time.Second, 50*time.Millisecond, "Should have flushed by ticker")
}

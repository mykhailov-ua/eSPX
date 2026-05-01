package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
	"github.com/stretchr/testify/mock"
)

type MockRetryQuerier struct {
	repository.Querier
	mock.Mock
}

func (m *MockRetryQuerier) InsertEventsBatch(ctx context.Context, arg repository.InsertEventsBatchParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func TestProcessor_RetrySuccess(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	origWait := ads.InitialWait
	origMax := ads.MaxRetries
	ads.InitialWait = 1 * time.Millisecond
	ads.MaxRetries = 2
	defer func() {
		ads.InitialWait = origWait
		ads.MaxRetries = origMax
	}()

	mockRepo := new(MockRetryQuerier)
	mockRepo.On("InsertEventsBatch", mock.Anything, mock.Anything).Return(errors.New("db error")).Twice()
	mockRepo.On("InsertEventsBatch", mock.Anything, mock.Anything).Return(nil).Once()

	proc := ads.NewProcessor(mockRepo, rdb, "sr", "gr", "cr", 1, 1, 1*time.Second, 1*time.Second)
	proc.Start(context.Background())
	time.Sleep(100 * time.Millisecond)

	_ = proc.Process(ads.Event{CampaignID: uuid.New(), Type: "click"})

	time.Sleep(500 * time.Millisecond)
	proc.Close()
	proc.Wait()

	mockRepo.AssertExpectations(t)
}

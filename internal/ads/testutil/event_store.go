package testutil

import (
	"context"
	"sync"

	"espx/internal/domain"
)

// MockEventStore records flushes for stream consumer tests.
type MockEventStore struct {
	mu      sync.Mutex
	Flushes [][]*domain.Event
	Err     error
}

func (m *MockEventStore) StoreBatch(ctx context.Context, events []*domain.Event) error {
	if m.Err != nil {
		return m.Err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	batchCopy := make([]*domain.Event, len(events))
	copy(batchCopy, events)
	m.Flushes = append(m.Flushes, batchCopy)
	return nil
}

func (m *MockEventStore) Close() error { return nil }

// FlushCount returns how many batches were stored.
func (m *MockEventStore) FlushCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Flushes)
}

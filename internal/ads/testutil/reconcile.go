package testutil

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
)

// MockPostgresDB is a Postgres stub for snapshot recovery spend reconciliation tests.
type MockPostgresDB struct {
	mu           sync.RWMutex
	Spends       map[uuid.UUID]int64
	Limits       map[uuid.UUID]int64
	Idempotency  map[string]bool
	Healthy      atomic.Bool
	NetworkDelay atomic.Int64
}

func (m *MockPostgresDB) UpdateCampaignSpend(ctx context.Context, campaignID uuid.UUID, currentSpend int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.Healthy.Load() {
		return errors.New("postgres connection timeout")
	}
	m.Spends[campaignID] = currentSpend
	return nil
}

func (m *MockPostgresDB) GetCampaignBudgetLimit(ctx context.Context, campaignID uuid.UUID) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.Healthy.Load() {
		return 0, errors.New("postgres connection timeout")
	}
	return m.Limits[campaignID], nil
}

func (m *MockPostgresDB) GetCampaignSpend(ctx context.Context, campaignID uuid.UUID) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.Healthy.Load() {
		return 0, errors.New("postgres connection timeout")
	}
	return m.Spends[campaignID], nil
}

func (m *MockPostgresDB) MarkEventIdempotent(ctx context.Context, clickID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.Healthy.Load() {
		return false, errors.New("postgres connection timeout")
	}
	if m.Idempotency[clickID] {
		return false, nil
	}
	m.Idempotency[clickID] = true
	return true, nil
}

// MockClickHouseDB returns aggregated spend for recovery replay tests.
type MockClickHouseDB struct {
	mu     sync.RWMutex
	Events []*domain.Event
}

func (m *MockClickHouseDB) QueryEventsSince(ctx context.Context, since time.Time) ([]*domain.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []*domain.Event
	for _, e := range m.Events {
		if e.CreatedAt.After(since) {
			res = append(res, e)
		}
	}
	return res, nil
}

func (m *MockClickHouseDB) QueryAggregatedSpend(ctx context.Context, until time.Time) (map[uuid.UUID]int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[uuid.UUID]int64)
	for _, e := range m.Events {
		if !e.CreatedAt.After(until) {
			charge := int64(10_000)
			if e.Type == "impression" {
				charge = int64(1_000)
			}
			res[e.CampaignID] += charge
		}
	}
	return res, nil
}

func (m *MockClickHouseDB) LogEvent(e *domain.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()

	eCopy := &domain.Event{
		ClickID:    e.ClickID,
		CampaignID: e.CampaignID,
		Type:       e.Type,
		Payload:    e.Payload,
		IP:         e.IP,
		UA:         e.UA,
		CreatedAt:  e.CreatedAt,
	}
	m.Events = append(m.Events, eCopy)
}

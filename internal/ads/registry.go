package ads

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/repository"
)

// Registry maintains in-memory campaign IDs for O(1) validation in the hot path.
// This prevents batch failures due to Foreign Key violations during bulk inserts.
type Registry struct {
	repo repository.Querier
	ids  map[uuid.UUID]uuid.UUID
	mu   sync.RWMutex
	wg   sync.WaitGroup
}

func NewRegistry(repo repository.Querier) *Registry {
	return &Registry{
		ids:  make(map[uuid.UUID]uuid.UUID, 100_000),
		repo: repo,
	}
}

func (r *Registry) Exists(id uuid.UUID) bool {
	r.mu.RLock()
	_, ok := r.ids[id]
	r.mu.RUnlock()
	return ok
}

func (r *Registry) GetCustomerID(campaignID uuid.UUID) (uuid.UUID, bool) {
	r.mu.RLock()
	id, ok := r.ids[campaignID]
	r.mu.RUnlock()
	return id, ok
}

func (r *Registry) Add(id, customerID uuid.UUID) {
	r.mu.Lock()
	r.ids[id] = customerID
	r.mu.Unlock()
}

func (r *Registry) Sync(ctx context.Context) (int, error) {
	rows, err := r.repo.ListActiveCampaigns(ctx)
	if err != nil {
		return 0, err
	}

	fresh := make(map[uuid.UUID]uuid.UUID, len(rows))
	for _, row := range rows {
		fresh[uuid.UUID(row.ID.Bytes)] = uuid.UUID(row.CustomerID.Bytes)
	}

	r.mu.Lock()
	r.ids = fresh
	r.mu.Unlock()

	return len(fresh), nil
}

func (r *Registry) StartSync(ctx context.Context, interval time.Duration) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count, err := r.Sync(ctx)
				if err != nil {
					slog.Error("campaign registry sync failed", "error", err)
					continue
				}
				slog.Debug("campaign registry synced", "campaigns", count)
			}
		}
	}()
}

func (r *Registry) Wait() {
	r.wg.Wait()
}

package campaign

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/database/db"
)

// Registry maintains in-memory campaign IDs for O(1) validation in the hot path.
// This prevents batch failures due to Foreign Key violations during bulk inserts.
type Registry struct {
	mu   sync.RWMutex
	ids  map[uuid.UUID]struct{}
	repo db.Querier
	wg   sync.WaitGroup
}

func NewRegistry(repo db.Querier) *Registry {
	return &Registry{
		ids:  make(map[uuid.UUID]struct{}, 100_000),
		repo: repo,
	}
}

func (r *Registry) Exists(id uuid.UUID) bool {
	r.mu.RLock()
	_, ok := r.ids[id]
	r.mu.RUnlock()
	return ok
}

func (r *Registry) Add(id uuid.UUID) {
	r.mu.Lock()
	r.ids[id] = struct{}{}
	r.mu.Unlock()
}

func (r *Registry) Sync(ctx context.Context) (int, error) {
	rows, err := r.repo.ListCampaignIDs(ctx)
	if err != nil {
		return 0, err
	}

	fresh := make(map[uuid.UUID]struct{}, len(rows))
	for _, pgID := range rows {
		fresh[uuid.UUID(pgID.Bytes)] = struct{}{}
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

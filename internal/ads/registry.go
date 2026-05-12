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
type campaignInfo struct {
	customerID uuid.UUID
	status     repository.CampaignStatusType
}

type Registry struct {
	repo repository.Querier
	data map[uuid.UUID]campaignInfo
	mu   sync.RWMutex
	wg   sync.WaitGroup
}

func NewRegistry(repo repository.Querier) *Registry {
	return &Registry{
		data: make(map[uuid.UUID]campaignInfo, 100_000),
		repo: repo,
	}
}

func (r *Registry) Exists(id uuid.UUID) bool {
	r.mu.RLock()
	info, ok := r.data[id]
	r.mu.RUnlock()
	return ok && info.status == repository.CampaignStatusTypeACTIVE
}

func (r *Registry) GetCustomerID(campaignID uuid.UUID) (uuid.UUID, bool) {
	r.mu.RLock()
	info, ok := r.data[campaignID]
	r.mu.RUnlock()
	if !ok {
		return uuid.Nil, false
	}
	return info.customerID, true
}

func (r *Registry) Add(id, customerID uuid.UUID) {
	r.mu.Lock()
	r.data[id] = campaignInfo{customerID: customerID, status: repository.CampaignStatusTypeACTIVE}
	r.mu.Unlock()
}

func (r *Registry) Sync(ctx context.Context) (int, error) {
	rows, err := r.repo.ListActiveCampaigns(ctx)
	if err != nil {
		return 0, err
	}

	fresh := make(map[uuid.UUID]campaignInfo, len(rows))
	for _, row := range rows {
		fresh[uuid.UUID(row.ID.Bytes)] = campaignInfo{
			customerID: uuid.UUID(row.CustomerID.Bytes),
			status:     row.Status,
		}
	}

	r.mu.Lock()
	r.data = fresh
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

func (r *Registry) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

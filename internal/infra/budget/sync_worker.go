package budget

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
)

type SyncWorker struct {
	rdb          redis.Cmdable
	campaignRepo domain.CampaignRepository
	customerRepo domain.CustomerRepository
	interval     time.Duration
}

func NewSyncWorker(
	rdb redis.Cmdable,
	campaignRepo domain.CampaignRepository,
	customerRepo domain.CustomerRepository,
	interval time.Duration,
) *SyncWorker {
	return &SyncWorker{
		rdb:          rdb,
		campaignRepo: campaignRepo,
		customerRepo: customerRepo,
		interval:     interval,
	}
}

func (w *SyncWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.SyncAll(context.Background())
			return
		case <-ticker.C:
			w.SyncAll(ctx)
		}
	}
}

func (w *SyncWorker) SyncAll(ctx context.Context) {
	w.syncCampaigns(ctx)
	w.syncCustomers(ctx)
}

func (w *SyncWorker) syncCampaigns(ctx context.Context) {
	var cursor uint64
	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_campaigns", cursor, "", 100).Result()
		if err != nil {
			return
		}

		for _, idStr := range keys {
			id, err := uuid.Parse(idStr)
			if err != nil {
				continue
			}

			syncKey := "budget:sync:campaign:" + idStr
			amount, err := w.rdb.Get(ctx, syncKey).Float64()
			if err == redis.Nil || amount <= 0 {
				w.rdb.SRem(ctx, "budget:dirty_campaigns", idStr)
				continue
			}

			if err := w.campaignRepo.UpdateSpend(ctx, id, amount); err == nil {
				rem, _ := w.rdb.IncrByFloat(ctx, syncKey, -amount).Result()
				if rem <= 0 {
					w.rdb.SRem(ctx, "budget:dirty_campaigns", idStr)
				}
			}
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
}

func (w *SyncWorker) syncCustomers(ctx context.Context) {
	var cursor uint64
	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_customers", cursor, "", 100).Result()
		if err != nil {
			return
		}

		for _, idStr := range keys {
			id, err := uuid.Parse(idStr)
			if err != nil {
				continue
			}

			syncKey := "budget:sync:customer:" + idStr
			amount, err := w.rdb.Get(ctx, syncKey).Float64()
			if err == redis.Nil || amount <= 0 {
				w.rdb.SRem(ctx, "budget:dirty_customers", idStr)
				continue
			}

			if err := w.customerRepo.UpdateBalance(ctx, id, amount); err == nil {
				rem, _ := w.rdb.IncrByFloat(ctx, syncKey, -amount).Result()
				if rem <= 0 {
					w.rdb.SRem(ctx, "budget:dirty_customers", idStr)
				}
			}
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
}

package ads

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

type campaignInfo struct {
	campaign *domain.Campaign
	status   db.CampaignStatusType
}

type Registry struct {
	repo          db.Querier
	data          map[uuid.UUID]campaignInfo
	manuallyAdded map[uuid.UUID]bool
	mu            sync.RWMutex
	wg            sync.WaitGroup
}

func NewRegistry(repo db.Querier) *Registry {
	return &Registry{
		data:          make(map[uuid.UUID]campaignInfo, 100_000),
		manuallyAdded: make(map[uuid.UUID]bool),
		repo:          repo,
	}
}

func (r *Registry) Exists(id uuid.UUID) bool {
	r.mu.RLock()
	info, ok := r.data[id]
	r.mu.RUnlock()
	return ok && info.status == db.CampaignStatusTypeACTIVE
}

func (r *Registry) GetCustomerID(campaignID uuid.UUID) (uuid.UUID, bool) {
	r.mu.RLock()
	info, ok := r.data[campaignID]
	r.mu.RUnlock()
	if !ok {
		return uuid.Nil, false
	}
	return info.campaign.CustomerID, true
}

func (r *Registry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	r.mu.RLock()
	info, ok := r.data[id]
	r.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return info.campaign, true
}

func (r *Registry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode domain.PacingMode, dailyBudget decimal.Decimal, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		slog.Error("invalid timezone in registry Add", "timezone", timezone, "error", err)
		loc = time.UTC
	}

	var countries []string
	if targetCountries != nil {
		countries = make([]string, len(targetCountries))
		copy(countries, targetCountries)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	info := campaignInfo{
		campaign: &domain.Campaign{
			ID:               id,
			IDStr:            id.String(),
			CustomerID:       customerID,
			CustomerIDStr:    customerID.String(),
			BrandID:          brandID,
			BrandFcapKey:     brandFcapKey,
			PacingMode:       pacingMode,
			DailyBudget:      dailyBudget,
			DailyBudgetMicro: DecimalToMicro(dailyBudget),
			Timezone:         timezone,
			Location:         loc,
			FreqLimit:        freqLimit,
			FreqWindow:       freqWindow,
			TargetCountries:  countries,
		},
		status: db.CampaignStatusTypeACTIVE,
	}
	r.data[id] = info
	r.manuallyAdded[id] = true
}

func (r *Registry) Sync(ctx context.Context) (int, error) {
	rows, err := r.repo.ListActiveCampaigns(ctx)
	if err != nil {
		return 0, err
	}

	fresh := make(map[uuid.UUID]campaignInfo, len(rows))
	for _, row := range rows {
		id := uuid.UUID(row.ID.Bytes)

		loc, err := time.LoadLocation(row.Timezone)
		if err != nil {
			slog.Warn("failed to load location, fallback to UTC", "campaign", id, "timezone", row.Timezone)
			loc = time.UTC
		}

		customerID := uuid.UUID(row.CustomerID.Bytes)
		dailyBudgetDec := FromNumeric(row.DailyBudget)

		var brandIDPtr *uuid.UUID
		if row.BrandID.Valid {
			brandID := uuid.UUID(row.BrandID.Bytes)
			brandIDPtr = &brandID
		}

		fresh[id] = campaignInfo{
			campaign: &domain.Campaign{
				ID:               id,
				IDStr:            id.String(),
				CustomerID:       customerID,
				CustomerIDStr:    customerID.String(),
				BrandID:          brandIDPtr,
				BrandFcapKey:     row.BrandFcapKey,
				PacingMode:       domain.PacingMode(row.PacingMode),
				DailyBudget:      dailyBudgetDec,
				DailyBudgetMicro: DecimalToMicro(dailyBudgetDec),
				Timezone:         row.Timezone,
				Location:         loc,
				FreqLimit:        row.FreqLimit.Int32,
				FreqWindow:       row.FreqWindow.Int32,
				TargetCountries:  row.TargetCountries,
			},
			status: row.Status,
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range fresh {
		delete(r.manuallyAdded, id)
	}
	for id := range r.manuallyAdded {
		if info, ok := r.data[id]; ok {
			fresh[id] = info
		}
	}
	r.data = fresh
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

func (r *Registry) StartWatch(ctx context.Context, rdb redis.UniversalClient, channel string) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		pubsub := rdb.Subscribe(ctx, channel)
		defer pubsub.Close()

		ch := pubsub.Channel(redis.WithChannelSize(1000))
		syncTrigger := make(chan struct{}, 1)

		// Listening to Redis PubSub and signaling syncTrigger allows instant cache invalidation.
		// Throttling DB re-sync (100ms sleep) protects PostgreSQL from connection exhaustion during high-concurrency campaigns update storms.
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-syncTrigger:
					count, err := r.Sync(ctx)
					if err != nil {
						slog.Error("live campaign registry sync failed", "error", err)
					} else {
						slog.Debug("live campaign registry synced via trigger", "campaigns", count)
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					slog.Error("redis pubsub channel closed permanently")
					return
				}
				id, err := uuid.Parse(msg.Payload)
				if err != nil {
					slog.Warn("received invalid campaign id in pubsub", "payload", msg.Payload)
					continue
				}
				select {
				case syncTrigger <- struct{}{}:
				default:
				}
				slog.Debug("registry sync triggered via pubsub", "campaign_id", id)
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

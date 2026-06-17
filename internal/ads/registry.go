package ads

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/ads/db"
	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	redis "github.com/redis/go-redis/v9"
)

// campaignInfo pairs a domain campaign with its registry sync status.
type campaignInfo struct {
	campaign *domain.Campaign
	status   db.CampaignStatusType
}

// CampaignRegistry is the lock-free in-memory source of campaign metadata for the hot path.
type CampaignRegistry struct {
	repo          db.Querier
	data          atomic.Value
	manuallyAdded map[uuid.UUID]bool
	mu            sync.Mutex
	replicaPath   string
	wg            sync.WaitGroup
	budgetWarmer  *BudgetCacheWarmer
}

// campaignReplicaDTO serializes registry snapshots to disk for cold-start recovery.
type campaignReplicaDTO struct {
	ID               uuid.UUID             `json:"id"`
	CustomerID       uuid.UUID             `json:"customer_id"`
	BrandID          *uuid.UUID            `json:"brand_id,omitempty"`
	BrandFcapKey     string                `json:"brand_fcap_key,omitempty"`
	Name             string                `json:"name"`
	BudgetLimit      int64                 `json:"budget_limit"`
	CurrentSpend     int64                 `json:"current_spend"`
	Status           domain.CampaignStatus `json:"status"`
	PacingMode       domain.PacingMode     `json:"pacing_mode"`
	DailyBudget      int64                 `json:"daily_budget"`
	DailyBudgetMicro int64                 `json:"daily_budget_micro"`
	Timezone         string                `json:"timezone"`
	FreqLimit        int32                 `json:"freq_limit"`
	FreqWindow       int32                 `json:"freq_window"`
	TargetCountries  []string              `json:"target_countries,omitempty"`
	StartAt          *time.Time            `json:"start_at,omitempty"`
	EndAt            *time.Time            `json:"end_at,omitempty"`
	DaypartHours     []int16               `json:"daypart_hours,omitempty"`
	RegistryStatus   string                `json:"registry_status"`
}

// NewRegistry creates an empty registry backed by Postgres sync and optional file replica.
func NewRegistry(repo db.Querier) *CampaignRegistry {
	r := &CampaignRegistry{
		manuallyAdded: make(map[uuid.UUID]bool),
		repo:          repo,
		replicaPath:   "campaigns_replica.json",
	}
	r.data.Store(make(map[uuid.UUID]campaignInfo, 100_000))
	return r
}

// SetReplicaPath configures where local JSON snapshots are written on sync.
func (r *CampaignRegistry) SetReplicaPath(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicaPath = path
}

// SetBudgetWarmer attaches the Redis budget warmer invoked after registry sync.
func (r *CampaignRegistry) SetBudgetWarmer(w *BudgetCacheWarmer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.budgetWarmer = w
}

// ActiveCampaigns returns ACTIVE campaigns from the current registry snapshot.
func (r *CampaignRegistry) ActiveCampaigns() []*domain.Campaign {
	m, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	if len(m) == 0 {
		return nil
	}
	out := make([]*domain.Campaign, 0, len(m))
	for _, info := range m {
		if info.status == db.CampaignStatusTypeACTIVE && info.campaign != nil {
			out = append(out, info.campaign)
		}
	}
	return out
}

// warmBudgetCache seeds Redis budget keys after a full registry reload.
func (r *CampaignRegistry) warmBudgetCache(ctx context.Context) {
	r.mu.Lock()
	w := r.budgetWarmer
	r.mu.Unlock()
	if w == nil {
		return
	}
	n, err := w.WarmFromRegistry(ctx, r)
	if err != nil {
		slog.Error("budget cache warm failed", "error", err)
		return
	}
	if n > 0 {
		slog.Debug("budget cache warmed", "keys_inserted", n)
	}
}

// Exists reports whether an ACTIVE campaign is present in the current snapshot.
func (r *CampaignRegistry) Exists(id uuid.UUID) bool {
	m, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	if m == nil {
		return false
	}
	info, ok := m[id]
	return ok && info.status == db.CampaignStatusTypeACTIVE
}

// GetCustomerID resolves the billing customer for a campaign id.
func (r *CampaignRegistry) GetCustomerID(campaignID uuid.UUID) (uuid.UUID, bool) {
	m, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	if m == nil {
		return uuid.Nil, false
	}
	info, ok := m[campaignID]
	if !ok {
		return uuid.Nil, false
	}
	return info.campaign.CustomerID, true
}

// GetCampaign returns the campaign snapshot; callers must not mutate the pointer.
func (r *CampaignRegistry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	m, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	if m == nil {
		return nil, false
	}
	info, ok := m[id]
	if !ok {
		return nil, false
	}
	return info.campaign, true
}

// Add inserts a manually registered campaign into the in-memory snapshot.
func (r *CampaignRegistry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode domain.PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		slog.Error("invalid timezone in registry Add", "timezone", timezone, "error", err)
		loc = time.UTC
	}

	var countries map[string]struct{}
	if targetCountries != nil {
		countries = make(map[string]struct{}, len(targetCountries))
		for _, c := range targetCountries {
			countries[c] = struct{}{}
		}
	}

	idStr := id.String()
	customerIDStr := customerID.String()
	dailyBudgetMicro := dailyBudget

	var fcapPrefix string
	if brandFcapKey != "" {
		fcapPrefix = brandFcapKey + ":u:"
	} else {
		fcapPrefix = "fcap:c:" + idStr + ":u:"
	}

	info := campaignInfo{
		campaign: &domain.Campaign{
			ID:                  id,
			IDStr:               idStr,
			IDStrAny:            idStr,
			CustomerID:          customerID,
			CustomerIDStr:       customerIDStr,
			CustomerIDStrAny:    customerIDStr,
			BrandID:             brandID,
			BrandFcapKey:        brandFcapKey,
			PacingMode:          pacingMode,
			DailyBudget:         dailyBudget,
			DailyBudgetMicro:    dailyBudgetMicro,
			DailyBudgetMicroAny: dailyBudgetMicro,
			Timezone:            timezone,
			Location:            loc,
			FreqLimit:           freqLimit,
			FreqLimitAny:        freqLimit,
			FreqWindow:          freqWindow,
			FreqWindowAny:       freqWindow,
			TargetCountries:     countries,
			BudgetCampaignKey:   "budget:campaign:" + idStr,
			CampaignSyncKey:     "budget:sync:campaign:" + idStr,
			CustomerSyncKey:     "budget:sync:customer:" + customerIDStr,
			FcapKeyPrefix:       fcapPrefix,
			DailySpendKeyPrefix: "budget:daily_spent:campaign:" + idStr + ":",
		},
		status: db.CampaignStatusTypeACTIVE,
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	currentMap, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	newMap := make(map[uuid.UUID]campaignInfo, len(currentMap)+1)
	for k, v := range currentMap {
		newMap[k] = v
	}

	newMap[id] = info
	r.manuallyAdded[id] = true
	r.data.Store(newMap)

	if err := r.saveReplica(newMap); err != nil {
		slog.Error("failed to save local file replica in Add", "error", err)
	}
}

// Sync reloads active campaigns from Postgres and preserves manually added entries.
func (r *CampaignRegistry) Sync(ctx context.Context) (int, error) {
	rows, err := r.repo.ListActiveCampaigns(ctx)
	if err != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		currentMap, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
		if len(currentMap) == 0 {
			slog.Warn("postgres sync failed and memory cache is empty, attempting to load from local file replica")
			if loadedMap, loadErr := r.loadReplica(); loadErr == nil {
				r.data.Store(loadedMap)
				return len(loadedMap), nil
			} else {
				slog.Error("failed to load from local file replica", "error", loadErr)
			}
		}
		return 0, err
	}

	fresh := make(map[uuid.UUID]campaignInfo, len(rows))
	for _, row := range rows {
		id := uuid.UUID(row.ID.Bytes)

		if row.UpdatedAt.Valid {
			lag := time.Since(row.UpdatedAt.Time).Seconds()
			if lag >= 0 {
				metrics.RegistrySyncLag.Observe(lag)
			}
		}

		customerID := uuid.UUID(row.CustomerID.Bytes)

		var brandIDPtr *uuid.UUID
		if row.BrandID.Valid {
			brandID := uuid.UUID(row.BrandID.Bytes)
			brandIDPtr = &brandID
		}

		camp := buildDomainCampaign(id, customerID, brandIDPtr, row.BrandFcapKey, row.PacingMode, row.DailyBudget, row.Timezone, row.FreqLimit.Int32, row.FreqWindow.Int32, row.TargetCountries, row.StartAt, row.EndAt, row.DaypartHours)
		camp.Name = row.Name
		camp.BudgetLimit = row.BudgetLimit
		camp.CurrentSpend = row.CurrentSpend
		camp.Status = domain.CampaignStatus(row.Status)
		fresh[id] = campaignInfo{
			campaign: camp,
			status:   row.Status,
		}
	}

	r.mu.Lock()
	for id := range fresh {
		delete(r.manuallyAdded, id)
	}
	currentMap, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	for id := range r.manuallyAdded {
		if info, ok := currentMap[id]; ok {
			fresh[id] = info
		}
	}
	r.data.Store(fresh)
	r.mu.Unlock()

	if err := r.saveReplica(fresh); err != nil {
		slog.Error("failed to save local file replica in Sync", "error", err)
	}

	r.warmBudgetCache(ctx)

	return len(fresh), nil
}

// saveReplica atomically writes the registry snapshot to the configured replica file.
func (r *CampaignRegistry) saveReplica(m map[uuid.UUID]campaignInfo) error {
	dtos := make([]campaignReplicaDTO, 0, len(m))
	for _, info := range m {
		var targetCountries []string
		if info.campaign.TargetCountries != nil {
			targetCountries = make([]string, 0, len(info.campaign.TargetCountries))
			for c := range info.campaign.TargetCountries {
				targetCountries = append(targetCountries, c)
			}
		}

		dtos = append(dtos, campaignReplicaDTO{
			ID:               info.campaign.ID,
			CustomerID:       info.campaign.CustomerID,
			BrandID:          info.campaign.BrandID,
			BrandFcapKey:     info.campaign.BrandFcapKey,
			Name:             info.campaign.Name,
			BudgetLimit:      info.campaign.BudgetLimit,
			CurrentSpend:     info.campaign.CurrentSpend,
			Status:           info.campaign.Status,
			PacingMode:       info.campaign.PacingMode,
			DailyBudget:      info.campaign.DailyBudget,
			DailyBudgetMicro: info.campaign.DailyBudgetMicro,
			Timezone:         info.campaign.Timezone,
			FreqLimit:        info.campaign.FreqLimit,
			FreqWindow:       info.campaign.FreqWindow,
			TargetCountries:  targetCountries,
			RegistryStatus:   string(info.status),
		})
	}

	data, err := json.Marshal(dtos)
	if err != nil {
		return err
	}

	tempFile := r.replicaPath + ".tmp"
	f, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {

		if !strings.HasPrefix(r.replicaPath, "/tmp/") {
			r.replicaPath = "/tmp/campaigns_replica.json"
			tempFile = r.replicaPath + ".tmp"
			f, err = os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		}
		if err != nil {
			return err
		}
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tempFile)
	}()

	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tempFile, r.replicaPath)
}

// loadReplica reads a JSON registry snapshot from disk for cold-start fallback.
func (r *CampaignRegistry) loadReplica() (map[uuid.UUID]campaignInfo, error) {
	data, err := os.ReadFile(r.replicaPath)
	if err != nil {
		if !strings.HasPrefix(r.replicaPath, "/tmp/") {
			data, err = os.ReadFile("/tmp/campaigns_replica.json")
		}
		if err != nil {
			return nil, err
		}
	}

	var dtos []campaignReplicaDTO
	if err := json.Unmarshal(data, &dtos); err != nil {
		return nil, err
	}

	m := make(map[uuid.UUID]campaignInfo, len(dtos))
	for _, dto := range dtos {
		loc, err := time.LoadLocation(dto.Timezone)
		if err != nil {
			loc = time.UTC
		}

		var countries map[string]struct{}
		if dto.TargetCountries != nil {
			countries = make(map[string]struct{}, len(dto.TargetCountries))
			for _, c := range dto.TargetCountries {
				countries[c] = struct{}{}
			}
		}

		idStr := dto.ID.String()
		customerIDStr := dto.CustomerID.String()

		var fcapPrefix string
		if dto.BrandFcapKey != "" {
			fcapPrefix = dto.BrandFcapKey + ":u:"
		} else {
			fcapPrefix = "fcap:c:" + idStr + ":u:"
		}

		m[dto.ID] = campaignInfo{
			campaign: &domain.Campaign{
				ID:                  dto.ID,
				IDStr:               idStr,
				IDStrAny:            idStr,
				CustomerID:          dto.CustomerID,
				CustomerIDStr:       customerIDStr,
				CustomerIDStrAny:    customerIDStr,
				BrandID:             dto.BrandID,
				BrandFcapKey:        dto.BrandFcapKey,
				Name:                dto.Name,
				BudgetLimit:         dto.BudgetLimit,
				CurrentSpend:        dto.CurrentSpend,
				Status:              dto.Status,
				PacingMode:          dto.PacingMode,
				DailyBudget:         dto.DailyBudget,
				DailyBudgetMicro:    dto.DailyBudgetMicro,
				DailyBudgetMicroAny: dto.DailyBudgetMicro,
				Timezone:            dto.Timezone,
				Location:            loc,
				FreqLimit:           dto.FreqLimit,
				FreqLimitAny:        dto.FreqLimit,
				FreqWindow:          dto.FreqWindow,
				FreqWindowAny:       dto.FreqWindow,
				TargetCountries:     countries,
				BudgetCampaignKey:   "budget:campaign:" + idStr,
				CampaignSyncKey:     "budget:sync:campaign:" + idStr,
				CustomerSyncKey:     "budget:sync:customer:" + customerIDStr,
				FcapKeyPrefix:       fcapPrefix,
				DailySpendKeyPrefix: "budget:daily_spent:campaign:" + idStr + ":",
			},
			status: db.CampaignStatusType(dto.RegistryStatus),
		}
	}
	return m, nil
}

// buildDomainCampaign constructs a hot-path campaign struct from Postgres row fields.
func buildDomainCampaign(
	id, customerID uuid.UUID,
	brandID *uuid.UUID,
	brandFcapKey string,
	pacingMode db.PacingModeType,
	dailyBudget int64,
	timezone string,
	freqLimit, freqWindow int32,
	targetCountries []string,
	startAt, endAt pgtype.Timestamptz,
	daypartHours []int16,
) *domain.Campaign {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	idStr := id.String()
	customerIDStr := customerID.String()
	var fcapPrefix string
	if brandFcapKey != "" {
		fcapPrefix = brandFcapKey + ":u:"
	} else {
		fcapPrefix = "fcap:c:" + idStr + ":u:"
	}
	var startPtr, endPtr *time.Time
	if startAt.Valid {
		t := startAt.Time
		startPtr = &t
	}
	if endAt.Valid {
		t := endAt.Time
		endPtr = &t
	}
	return &domain.Campaign{
		ID:                  id,
		IDStr:               idStr,
		IDStrAny:            idStr,
		CustomerID:          customerID,
		CustomerIDStr:       customerIDStr,
		CustomerIDStrAny:    customerIDStr,
		BrandID:             brandID,
		BrandFcapKey:        brandFcapKey,
		PacingMode:          domain.PacingMode(pacingMode),
		DailyBudget:         dailyBudget,
		DailyBudgetMicro:    dailyBudget,
		DailyBudgetMicroAny: dailyBudget,
		Timezone:            timezone,
		Location:            loc,
		FreqLimit:           freqLimit,
		FreqLimitAny:        freqLimit,
		FreqWindow:          freqWindow,
		FreqWindowAny:       freqWindow,
		TargetCountries:     SliceToMap(targetCountries),
		BudgetCampaignKey:   "budget:campaign:" + idStr,
		CampaignSyncKey:     "budget:sync:campaign:" + idStr,
		CustomerSyncKey:     "budget:sync:customer:" + customerIDStr,
		FcapKeyPrefix:       fcapPrefix,
		DailySpendKeyPrefix: "budget:daily_spent:campaign:" + idStr + ":",
		StartAt:             startPtr,
		EndAt:               endPtr,
		DaypartHours:        DaypartSliceToSet(daypartHours),
	}
}

// StartSync runs periodic Postgres registry refresh until the context is cancelled.
func (r *CampaignRegistry) StartSync(ctx context.Context, interval time.Duration) {
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

// UpdateAndWarmCampaign refreshes one campaign from Postgres and warms its Redis budget key.
func (r *CampaignRegistry) UpdateAndWarmCampaign(ctx context.Context, id uuid.UUID) error {
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	row, err := r.repo.GetCampaignBudget(ctx, pgID)
	if err != nil {
		return fmt.Errorf("failed to get campaign budget from pg: %w", err)
	}

	r.mu.Lock()
	currentMap, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	info, exists := currentMap[id]

	var camp *domain.Campaign
	if exists {
		clonedCamp := *info.campaign
		clonedCamp.BudgetLimit = row.BudgetLimit
		clonedCamp.CurrentSpend = row.CurrentSpend
		clonedCamp.Status = domain.CampaignStatus(row.Status)

		camp = &clonedCamp

		newMap := make(map[uuid.UUID]campaignInfo, len(currentMap))
		for k, v := range currentMap {
			newMap[k] = v
		}
		newMap[id] = campaignInfo{
			campaign: camp,
			status:   row.Status,
		}
		r.data.Store(newMap)

		if err := r.saveReplica(newMap); err != nil {
			slog.Error("failed to save local file replica in UpdateAndWarmCampaign", "error", err)
		}
	} else {
		idStr := id.String()
		camp = &domain.Campaign{
			ID:                id,
			IDStr:             idStr,
			IDStrAny:          idStr,
			CustomerID:        uuid.UUID(row.CustomerID.Bytes),
			BudgetLimit:       row.BudgetLimit,
			CurrentSpend:      row.CurrentSpend,
			Status:            domain.CampaignStatus(row.Status),
			BudgetCampaignKey: "budget:campaign:" + idStr,
		}
	}
	w := r.budgetWarmer
	r.mu.Unlock()

	if row.Status == db.CampaignStatusTypeACTIVE && w != nil {
		warmed, err := w.WarmOne(ctx, camp)
		if err != nil {
			return fmt.Errorf("failed to warm single campaign budget: %w", err)
		}
		if warmed {
			slog.Debug("single campaign budget cache warmed via pubsub", "campaign_id", id)
		}
	}
	return nil
}

// StartWatch listens for campaign change pubsub messages and triggers incremental sync.
func (r *CampaignRegistry) StartWatch(ctx context.Context, rdb redis.UniversalClient, channel string) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		pubsub := rdb.Subscribe(ctx, channel)
		defer pubsub.Close()

		ch := pubsub.Channel(redis.WithChannelSize(1000))
		syncTrigger := make(chan struct{}, 1)

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

				go func(cid uuid.UUID) {
					if err := r.UpdateAndWarmCampaign(ctx, cid); err != nil {
						slog.Error("failed to incremental warm campaign via pubsub", "campaign_id", cid, "error", err)
					}
				}(id)

				select {
				case syncTrigger <- struct{}{}:
				default:
				}
				slog.Debug("registry sync triggered via pubsub", "campaign_id", id)
			}
		}
	}()
}

// Wait blocks until background registry goroutines exit or the context is cancelled.
func (r *CampaignRegistry) Wait(ctx context.Context) error {
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

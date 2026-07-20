package ingestion

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/campaignmodel"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/licensing"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	redis "github.com/redis/go-redis/v9"
)

type campaignInfo struct {
	campaign *campaignmodel.Campaign
	status   db.CampaignStatusType
}

type campaignMapSnapshot struct {
	byID map[uuid.UUID]campaignInfo
}

func (r *Registry) campaignMapSnapshot() *campaignMapSnapshot {
	v, ok := r.data.Load().(*campaignMapSnapshot)
	if !ok || v == nil {
		return &campaignMapSnapshot{}
	}
	return v
}

type campaignReplicaDTO struct {
	ID               uuid.UUID                    `json:"id"`
	CustomerID       uuid.UUID                    `json:"customer_id"`
	BrandID          *uuid.UUID                   `json:"brand_id,omitempty"`
	BrandFcapKey     string                       `json:"brand_fcap_key,omitempty"`
	Name             string                       `json:"name"`
	BudgetLimit      int64                        `json:"budget_limit"`
	CurrentSpend     int64                        `json:"current_spend"`
	Status           campaignmodel.CampaignStatus `json:"status"`
	PacingMode       campaignmodel.PacingMode     `json:"pacing_mode"`
	DailyBudget      int64                        `json:"daily_budget"`
	DailyBudgetMicro int64                        `json:"daily_budget_micro"`
	Timezone         string                       `json:"timezone"`
	FreqLimit        int32                        `json:"freq_limit"`
	FreqWindow       int32                        `json:"freq_window"`
	TargetCountries  []string                     `json:"target_countries,omitempty"`
	RegistryStatus   string                       `json:"registry_status"`
}

type entitlementsSnapshot struct {
	byCustomerID map[uuid.UUID]licensing.Entitlements
	license      licensing.Entitlements
	licenseState licensing.LicenseState
}

// Registry maintains an in-memory replica of active campaigns for lock-free hot-path reads.
//
// Reads (Exists, GetCampaign, GetCustomerID) use atomic.Value map swaps with no mutex.
// Writes (Add, Sync) clone the map under mu and publish a new pointer for readers.
type Registry struct {
	repo          db.Querier
	pool          *pgxpool.Pool
	data          atomic.Value // holds *campaignMapSnapshot
	entitlements  atomic.Value // holds *entitlementsSnapshot
	manuallyAdded map[uuid.UUID]bool
	mu            sync.Mutex // guards writes (updates to data map, manuallyAdded)
	replicaPath   string
	wg            sync.WaitGroup
	budgetWarmer  *BudgetCacheWarmer
}

func NewRegistry(repo db.Querier) *Registry {
	r := &Registry{
		manuallyAdded: make(map[uuid.UUID]bool),
		repo:          repo,
		replicaPath:   "campaigns_replica.json",
	}
	r.data.Store(&campaignMapSnapshot{byID: make(map[uuid.UUID]campaignInfo, 100_000)})
	r.entitlements.Store(&entitlementsSnapshot{
		byCustomerID: make(map[uuid.UUID]licensing.Entitlements),
		licenseState: licensing.StateExpired,
	})
	return r
}

func (r *Registry) SetPool(pool *pgxpool.Pool) {
	r.pool = pool
}

// SetBudgetWarmer wires incremental Redis budget warming after registry updates.
func (r *Registry) SetBudgetWarmer(w *BudgetCacheWarmer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.budgetWarmer = w
}

// UpdateAndWarmCampaign reloads one campaign from Postgres and warms its Redis budget key.
func (r *Registry) UpdateAndWarmCampaign(ctx context.Context, id uuid.UUID) error {
	camp, err := NewCampaignRepo(r.repo).GetByID(ctx, id)
	if err != nil {
		return err
	}
	r.mu.Lock()
	currentMap := r.campaignMapSnapshot().byID
	newMap := make(map[uuid.UUID]campaignInfo, len(currentMap)+1)
	for k, v := range currentMap {
		newMap[k] = v
	}
	if info, ok := newMap[id]; ok && info.campaign != nil {
		info.campaign.BudgetLimit = camp.BudgetLimit
		info.campaign.CurrentSpend = camp.CurrentSpend
		info.campaign.DailyBudget = camp.DailyBudget
		info.campaign.DailyBudgetMicro = camp.DailyBudgetMicro
		info.campaign.DailyBudgetMicroAny = camp.DailyBudgetMicro
		newMap[id] = info
	}
	r.data.Store(&campaignMapSnapshot{byID: newMap})
	w := r.budgetWarmer
	r.mu.Unlock()
	if w == nil {
		return nil
	}
	_, err = w.WarmOne(ctx, camp)
	return err
}

func (r *Registry) SetReplicaPath(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicaPath = path
}

func (r *Registry) Exists(id uuid.UUID) bool {
	info, ok := r.campaignMapSnapshot().byID[id]
	return ok && info.status == db.CampaignStatusTypeACTIVE
}

func (r *Registry) GetCustomerID(campaignID uuid.UUID) (uuid.UUID, bool) {
	info, ok := r.campaignMapSnapshot().byID[campaignID]
	if !ok {
		return uuid.Nil, false
	}
	return info.campaign.CustomerID, true
}

func (r *Registry) GetCampaign(id uuid.UUID) (*campaignmodel.Campaign, bool) {
	info, ok := r.campaignMapSnapshot().byID[id]
	if !ok {
		return nil, false
	}
	return info.campaign, true
}

// ActiveCampaigns returns a snapshot of active campaigns for RTB sync and budget warm paths.
func (r *Registry) ActiveCampaigns() []*campaignmodel.Campaign {
	m := r.campaignMapSnapshot().byID
	if len(m) == 0 {
		return nil
	}
	out := make([]*campaignmodel.Campaign, 0, len(m))
	for _, info := range m {
		if info.status != db.CampaignStatusTypeACTIVE || info.campaign == nil {
			continue
		}
		out = append(out, info.campaign)
	}
	return out
}

func (r *Registry) Add(id, customerID uuid.UUID, brandID *uuid.UUID, brandFcapKey string, pacingMode campaignmodel.PacingMode, dailyBudget int64, timezone string, freqLimit, freqWindow int32, targetCountries []string) {
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
		campaign: &campaignmodel.Campaign{
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
			Status:              campaignmodel.CampaignStatusActive,
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

	currentMap := r.campaignMapSnapshot().byID
	newMap := make(map[uuid.UUID]campaignInfo, len(currentMap)+1)
	for k, v := range currentMap {
		newMap[k] = v
	}

	newMap[id] = info
	r.manuallyAdded[id] = true
	r.data.Store(&campaignMapSnapshot{byID: newMap})

	if err := r.saveReplica(newMap); err != nil {
		slog.Error("failed to save local file replica in Add", "error", err)
	}
}

// Sync reloads active campaigns from Postgres into a new map and atomically swaps readers.
// When Postgres is down and memory is empty, falls back to campaigns_replica.json on disk.
func (r *Registry) Sync(ctx context.Context) (int, error) {
	if r.pool != nil {
		if err := r.SyncEntitlements(ctx); err != nil {
			slog.Error("entitlements sync failed", "error", err)
		}
	}
	rows, err := r.repo.ListActiveCampaigns(ctx)
	if err != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		currentMap := r.campaignMapSnapshot().byID
		if len(currentMap) == 0 {
			slog.Warn("postgres sync failed and memory cache is empty, attempting to load from local file replica")
			if loaded, loadErr := r.loadReplica(); loadErr == nil {
				r.data.Store(loaded)
				return len(loaded.byID), nil
			} else {
				slog.Error("failed to load from local file replica", "error", loadErr)
			}
		}
		return 0, err
	}

	fresh := make(map[uuid.UUID]campaignInfo, len(rows))
	for _, row := range rows {
		id := uuid.UUID(row.ID.Bytes)

		// Measure the lag between the database-recorded UpdatedAt timestamp and the
		// current time at cache load. This captures end-to-end propagation delay from
		// management write -> PostgreSQL -> Sync -> in-memory registry.
		if row.UpdatedAt.Valid {
			lag := time.Since(row.UpdatedAt.Time).Seconds()
			if lag >= 0 {
				metrics.RegistrySyncLag.Observe(lag)
			}
		}

		fresh[id] = campaignInfo{
			campaign: campaignFromDBRow(row),
			status:   row.Status,
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for id := range fresh {
		delete(r.manuallyAdded, id)
	}
	currentMap := r.campaignMapSnapshot().byID
	for id := range r.manuallyAdded {
		if info, ok := currentMap[id]; ok {
			fresh[id] = info
		}
	}

	r.data.Store(&campaignMapSnapshot{byID: fresh})

	if err := r.saveReplica(fresh); err != nil {
		slog.Error("failed to save local file replica in Sync", "error", err)
	}

	return len(fresh), nil
}

func (r *Registry) saveReplica(m map[uuid.UUID]campaignInfo) error {
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
		// Auto-fallback to /tmp if primary directory is not writable
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

func (r *Registry) loadReplica() (*campaignMapSnapshot, error) {
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
			campaign: &campaignmodel.Campaign{
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
	return &campaignMapSnapshot{byID: m}, nil
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

func (r *Registry) SyncEntitlements(ctx context.Context) error {
	if r.pool == nil {
		return nil
	}

	var planCode string
	var stateStr string
	var entitlementsBytes []byte
	var validUntil time.Time

	var licEnt licensing.Entitlements
	var licState licensing.LicenseState = licensing.StateActive // Default active if not configured

	row := r.pool.QueryRow(ctx, "SELECT plan_code, state, entitlements_json, valid_until FROM billing.license_status LIMIT 1")
	err := row.Scan(&planCode, &stateStr, &entitlementsBytes, &validUntil)
	if err == nil {
		licState = licensing.LicenseState(stateStr)
		_ = json.Unmarshal(entitlementsBytes, &licEnt)
	} else {
		// Default limits for testing/development
		licEnt.Limits.MaxRPS = 999999
		licEnt.Limits.MaxRequestsPerDay = 999999999
		licEnt.Limits.MaxActiveCampaigns = 99999
		licEnt.Limits.MaxRegions = 9
		licEnt.Features.RtbLive = true
		licEnt.Features.MlFraudBoost = true
		licEnt.Features.MultiRegion = true
		licEnt.Features.SlotMigration = true
	}

	custRows, err := r.pool.Query(ctx, `
		SELECT s.customer_id, s.plan_code, s.status, p.limits_json, p.features_json, s.overrides_json
		FROM billing.customer_subscriptions s
		JOIN billing.subscription_plans p ON s.plan_code = p.code
	`)

	byCust := make(map[uuid.UUID]licensing.Entitlements)
	if err == nil {
		defer custRows.Close()
		for custRows.Next() {
			var custID uuid.UUID
			var planCode string
			var status string
			var limitsBytes []byte
			var featuresBytes []byte
			var overridesBytes []byte

			err := custRows.Scan(&custID, &planCode, &status, &limitsBytes, &featuresBytes, &overridesBytes)
			if err != nil {
				continue
			}

			var limits licensing.Limits
			_ = json.Unmarshal(limitsBytes, &limits)

			var features licensing.FeatureSet
			_ = json.Unmarshal(featuresBytes, &features)

			if len(overridesBytes) > 0 {
				var overrides struct {
					Limits   *licensing.Limits     `json:"limits,omitempty"`
					Features *licensing.FeatureSet `json:"features,omitempty"`
				}
				if json.Unmarshal(overridesBytes, &overrides) == nil {
					if overrides.Limits != nil {
						mergeLimits(&limits, *overrides.Limits)
					}
					if overrides.Features != nil {
						mergeFeatures(&features, *overrides.Features)
					}
				}
			}

			custEnt := licensing.Entitlements{
				Limits:   limits,
				Features: features,
			}

			byCust[custID] = licensing.Effective(licEnt, custEnt)
		}
	}

	r.entitlements.Store(&entitlementsSnapshot{
		byCustomerID: byCust,
		license:      licEnt,
		licenseState: licState,
	})

	return nil
}

func (r *Registry) GetEntitlements(customerID uuid.UUID) (licensing.Entitlements, bool) {
	snap, ok := r.entitlements.Load().(*entitlementsSnapshot)
	if !ok || snap == nil {
		return licensing.Entitlements{}, false
	}
	ent, ok := snap.byCustomerID[customerID]
	return ent, ok
}

func (r *Registry) GetLicenseState() (licensing.LicenseState, licensing.Entitlements) {
	snap, ok := r.entitlements.Load().(*entitlementsSnapshot)
	if !ok || snap == nil {
		return licensing.StateExpired, licensing.Entitlements{}
	}
	return snap.licenseState, snap.license
}

func mergeLimits(dst *licensing.Limits, src licensing.Limits) {
	if src.MaxRPS != 0 {
		dst.MaxRPS = src.MaxRPS
	}
	if src.MaxRequestsPerDay != 0 {
		dst.MaxRequestsPerDay = src.MaxRequestsPerDay
	}
	if src.MaxActiveCampaigns != 0 {
		dst.MaxActiveCampaigns = src.MaxActiveCampaigns
	}
	if src.MaxRegions != 0 {
		dst.MaxRegions = src.MaxRegions
	}
	if src.MaxTenants != 0 {
		dst.MaxTenants = src.MaxTenants
	}
	if src.MaxEventsPerMonth != 0 {
		dst.MaxEventsPerMonth = src.MaxEventsPerMonth
	}
	if src.MaxAPIKeys != 0 {
		dst.MaxAPIKeys = src.MaxAPIKeys
	}
	if src.MaxExportChunkBytes != 0 {
		dst.MaxExportChunkBytes = src.MaxExportChunkBytes
	}
	if src.QuotaResetTimezone != "" {
		dst.QuotaResetTimezone = src.QuotaResetTimezone
	}
}

func mergeFeatures(dst *licensing.FeatureSet, src licensing.FeatureSet) {
	dst.RtbLive = src.RtbLive
	dst.MlFraudBoost = src.MlFraudBoost
	dst.MultiRegion = src.MultiRegion
	dst.SlotMigration = src.SlotMigration
	dst.MarginGuard = src.MarginGuard
}

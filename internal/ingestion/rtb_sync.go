package ingestion

import (
	"context"
	"sort"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/config"

	"github.com/google/uuid"
)

const (
	rtbDeviceMaskAll    uint8  = 7
	rtbDaySeconds       int64  = 86400
	defaultHybridMaxRPS        = 5000
	defaultCategoryMask uint64 = 1
)

// BuildCampaignMetaList materializes hybrid balancer weights from active registry campaigns.
func BuildCampaignMetaList(campaigns []*campaignmodel.Campaign, cfg *config.Config) []*CampaignMeta {
	if len(campaigns) == 0 || cfg == nil {
		return nil
	}
	bidDefault := defaultBidMicro(cfg)
	out := make([]*CampaignMeta, 0, len(campaigns))
	for _, camp := range campaigns {
		if camp == nil {
			continue
		}
		total := camp.BudgetLimit
		if total < 0 {
			total = 0
		}
		out = append(out, &CampaignMeta{
			ID:                camp.ID,
			BidMicro:          bidDefault,
			CTR:               1.0,
			RemainingBudget:   RemainingBudgetMicro(camp),
			TotalBudget:       total,
			PeakTrafficFactor: 1.0,
		})
	}
	return out
}

func defaultBidMicro(cfg *config.Config) int64 {
	bidMicro := cfg.ClickAmount
	if bidMicro <= 0 {
		bidMicro = cfg.ImpressionAmount
	}
	if bidMicro <= 0 {
		bidMicro = 1
	}
	return bidMicro
}

func campaignMetaByID(metas []*CampaignMeta) map[uuid.UUID]*CampaignMeta {
	if len(metas) == 0 {
		return nil
	}
	out := make(map[uuid.UUID]*CampaignMeta, len(metas))
	for _, meta := range metas {
		if meta != nil {
			out[meta.ID] = meta
		}
	}
	return out
}

// buildCustomerBudgetPools sums remaining campaign budgets per customer for shared RTB pools.
func buildCustomerBudgetPools(campaigns []*campaignmodel.Campaign) map[uuid.UUID]int64 {
	if len(campaigns) == 0 {
		return nil
	}
	out := make(map[uuid.UUID]int64)
	for _, camp := range campaigns {
		if camp == nil || camp.CustomerID == uuid.Nil {
			continue
		}
		out[camp.CustomerID] += RemainingBudgetMicro(camp)
	}
	return out
}

// BuildRtbInputsFromRegistry materializes per-campaign auction catalog fields from registry snapshots.
func BuildRtbInputsFromRegistry(
	registry *Registry,
	cfg *config.Config,
	metaByID map[uuid.UUID]*CampaignMeta,
	customerPools map[uuid.UUID]int64,
	hybrid *HybridBalancer,
	boosts *FraudScoreBoostSnapshot,
) map[uuid.UUID]RtbCampaignInput {
	if registry == nil || cfg == nil {
		return nil
	}
	campaigns := registry.ActiveCampaigns()
	if len(campaigns) == 0 {
		return nil
	}
	out := make(map[uuid.UUID]RtbCampaignInput, len(campaigns))
	for _, camp := range campaigns {
		if camp == nil {
			continue
		}
		out[camp.ID] = rtbInputForCampaign(camp, cfg, metaByID[camp.ID], customerPools[camp.CustomerID], hybrid, boosts)
	}
	return out
}

func rtbInputForCampaign(
	camp *campaignmodel.Campaign,
	cfg *config.Config,
	meta *CampaignMeta,
	customerBudget int64,
	hybrid *HybridBalancer,
	boosts *FraudScoreBoostSnapshot,
) RtbCampaignInput {
	geo := firstTargetCountryGeo(camp)
	pacing := PacingOpenFromManagement(camp.Status == campaignmodel.CampaignStatusActive)
	customerID := CustomerIDFromCustomerUUID(camp.CustomerID)
	dailyMicro := camp.DailyBudgetMicro
	if dailyMicro <= 0 {
		dailyMicro = camp.DailyBudget
	}
	weight := uint32(1)
	if hybrid != nil {
		weight = hybrid.WeightFor(camp.ID)
	}
	boostPPM := uint32(CTRPPMUnit)
	if boosts != nil {
		if b, ok := boosts.Boosts[camp.ID]; ok {
			boostPPM = BoostPPMFromUint8(b)
		}
	}
	if meta != nil {
		inp := RtbCampaignInputFromHybrid(
			meta,
			geo,
			rtbDeviceMaskAll,
			defaultCategoryMask,
			weight,
			pacing,
			customerID,
			customerBudget,
			dailyMicro,
		)
		inp.ReserveMicro = camp.ReserveMicro
		inp.BoostPPM = boostPPM
		return inp
	}
	return RtbCampaignInput{
		BidMicro:         defaultBidMicro(cfg),
		CTRPPM:           CTRPPMUnit,
		ReserveMicro:     camp.ReserveMicro,
		DeviceMask:       rtbDeviceMaskAll,
		CategoryMask:     defaultCategoryMask,
		GeoHash:          geo,
		Weight:           weight,
		BoostPPM:         boostPPM,
		PacingOpen:       pacing,
		CustomerID:       customerID,
		CustomerBudget:   customerBudget,
		DailyBudgetMicro: dailyMicro,
	}
}

func firstTargetCountryGeo(camp *campaignmodel.Campaign) uint32 {
	if camp == nil || len(camp.TargetCountries) == 0 {
		return 0
	}
	countries := make([]string, 0, len(camp.TargetCountries))
	for c := range camp.TargetCountries {
		countries = append(countries, c)
	}
	sort.Strings(countries)
	return GeoHashFromCountry(countries[0])
}

// BudgetAuthorityFromConfig maps rollout config to rtb spend policy.
func BudgetAuthorityFromConfig(cfg *config.Config) BudgetAuthority {
	return BudgetAuthorityFromSettings(cfg, "")
}

func utcSecondsElapsed() int64 {
	now := time.Now().UTC()
	return int64(now.Hour()*3600 + now.Minute()*60 + now.Second())
}

// SyncRtbCatalog rebuilds the in-process RTB catalog from registry and optional hybrid metadata.
func SyncRtbCatalog(
	ctx context.Context,
	registry *Registry,
	catalog *RtbCatalog,
	cfg *config.Config,
	hybrid *HybridBalancer,
	budgetSync RtbBudgetSync,
	watcher *SettingsWatcher,
) {
	if registry == nil || catalog == nil || cfg == nil {
		return
	}
	campaigns := registry.ActiveCampaigns()
	metas := BuildCampaignMetaList(campaigns, cfg)
	metaByID := campaignMetaByID(metas)
	if hybrid != nil {
		hybrid.UpdateCampaigns(metas, utcSecondsElapsed(), rtbDaySeconds)
	}
	customerPools := buildCustomerBudgetPools(campaigns)
	boosts := fraudBoostsFromWatcher(watcher)
	inputs := BuildRtbInputsFromRegistry(registry, cfg, metaByID, customerPools, hybrid, boosts)
	rows := BuildRtbCatalogRowsFromHybrid(campaigns, metaByID, inputs)
	catalog.SyncCampaignRows(campaigns, rows)
	SyncRTBBudgetState(ctx, catalog.Registry().Store(), campaigns, customerPools, budgetSync)
}

// StartRtbCatalogSync rebuilds the in-process catalog on the registry sync interval.
func StartRtbCatalogSync(
	ctx context.Context,
	registry *Registry,
	catalog *RtbCatalog,
	cfg *config.Config,
	hybrid *HybridBalancer,
	budgetSync RtbBudgetSync,
	watcher *SettingsWatcher,
	interval time.Duration,
) {
	if registry == nil || catalog == nil || cfg == nil || interval <= 0 {
		return
	}
	syncOnce := func() {
		syncCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		SyncRtbCatalog(syncCtx, registry, catalog, cfg, hybrid, budgetSync, watcher)
		cancel()
	}
	syncOnce()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				syncOnce()
			}
		}
	}()
}

// HybridMaxRPSFromConfig returns the per-node hot-campaign threshold for hybrid sharding.
func HybridMaxRPSFromConfig(cfg *config.Config) int {
	if cfg == nil || cfg.RtbHybridMaxRpsPerNode <= 0 {
		return defaultHybridMaxRPS
	}
	return cfg.RtbHybridMaxRpsPerNode
}

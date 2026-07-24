package ingestion

import (
	"sync/atomic"

	"espx/internal/campaignmodel"
	"espx/internal/rtb"

	"github.com/google/uuid"
)

// RtbCatalog wires the in-process rtb registry to ads campaign sync and ingest targeting.
type RtbCatalog struct {
	registry   *rtb.Registry
	dealIndex  *rtb.DealIndex
	dealFloors *DealFloorCache
	authority  BudgetAuthority
	winnerUUID atomic.Pointer[map[rtb.CampaignID]uuid.UUID]

	prebidIVT       atomic.Bool
	schainAllow     atomic.Pointer[SupplyChainAllowlistSnapshot]
	settingsWatcher *SettingsWatcher
	ingestGeo       GeoProvider
}

func NewRtbCatalog(store *rtb.BudgetStore, authority BudgetAuthority) *RtbCatalog {
	return &RtbCatalog{
		registry:  rtb.NewRegistry(store),
		dealIndex: rtb.NewDealIndex(),
		authority: authority,
	}
}

func (catalog *RtbCatalog) Registry() *rtb.Registry {
	return catalog.registry
}

func (catalog *RtbCatalog) Authority() BudgetAuthority {
	return catalog.authority
}

// SetAuthority updates live budget ownership without rebuilding the catalog.
func (catalog *RtbCatalog) SetAuthority(authority BudgetAuthority) {
	catalog.authority = authority
}

// SetPrebidIVT enables the pre-bid IVT gate before RunAuction (R17).
func (catalog *RtbCatalog) SetPrebidIVT(enabled bool) {
	catalog.prebidIVT.Store(enabled)
}

// SetSupplyChainAllowlist installs the hot-path schain allowlist snapshot (R18).
func (catalog *RtbCatalog) SetSupplyChainAllowlist(snap *SupplyChainAllowlistSnapshot) {
	if snap == nil {
		catalog.schainAllow.Store(nil)
		return
	}
	catalog.schainAllow.Store(snap)
}

// ConfigureRtbGates wires prefilter dependencies for live auction paths.
func (catalog *RtbCatalog) ConfigureRtbGates(watcher *SettingsWatcher, geo GeoProvider) {
	if catalog == nil {
		return
	}
	catalog.settingsWatcher = watcher
	catalog.ingestGeo = geo
}

// SetDealFloors attaches the read-only Redis-backed optimized floor cache.
func (catalog *RtbCatalog) SetDealFloors(cache *DealFloorCache) {
	catalog.dealFloors = cache
}

// SyncActiveCampaigns rebuilds the rtb catalog from active domain campaigns and auction inputs.
func (catalog *RtbCatalog) SyncActiveCampaigns(campaigns []*campaignmodel.Campaign, inputs map[uuid.UUID]RtbCampaignInput) {
	rows := BuildRtbCatalogRows(campaigns, inputs)
	catalog.registry.UpdateCampaigns(rows)
	catalog.rebuildWinnerUUID(rows, campaigns)
}

func (catalog *RtbCatalog) rebuildWinnerUUID(rows []rtb.CampaignData, campaigns []*campaignmodel.Campaign) {
	if len(rows) == 0 {
		empty := make(map[rtb.CampaignID]uuid.UUID)
		catalog.winnerUUID.Store(&empty)
		return
	}
	m := make(map[rtb.CampaignID]uuid.UUID, len(rows))
	for _, camp := range campaigns {
		if camp == nil {
			continue
		}
		m[CampaignIDFromUUID(camp.ID)] = camp.ID
	}
	catalog.winnerUUID.Store(&m)
}

// UUIDForWinner resolves the domain campaign UUID for an auction winner key.
func (catalog *RtbCatalog) UUIDForWinner(id rtb.CampaignID) (uuid.UUID, bool) {
	ptr := catalog.winnerUUID.Load()
	if ptr == nil {
		return uuid.Nil, false
	}
	uid, ok := (*ptr)[id]
	return uid, ok
}

func (catalog *RtbCatalog) SyncCampaignRows(campaigns []*campaignmodel.Campaign, rows []rtb.CampaignData) {
	catalog.registry.UpdateCampaigns(rows)
	catalog.rebuildWinnerUUID(rows, campaigns)
}

// SyncFromRegistry reloads the rtb catalog from an ads CampaignRegistry snapshot.
func (catalog *RtbCatalog) SyncFromRegistry(registry *Registry, inputs map[uuid.UUID]RtbCampaignInput) {
	if registry == nil {
		catalog.registry.UpdateCampaigns(nil)
		return
	}
	catalog.SyncActiveCampaigns(registry.ActiveCampaigns(), inputs)
}

func (catalog *RtbCatalog) SetClearingMode(mode rtb.ClearingMode) {
	catalog.registry.SetClearingMode(mode)
}

// UpdateDeals rebuilds the PMP deal index from Postgres rows.
func (catalog *RtbCatalog) UpdateDeals(deals []rtb.DealData) {
	if catalog.dealIndex == nil {
		catalog.dealIndex = rtb.NewDealIndex()
	}
	catalog.dealIndex.UpdateDeals(deals)
}

// DealCount returns the number of indexed PMP deals.
func (catalog *RtbCatalog) DealCount() int {
	if catalog.dealIndex == nil {
		return 0
	}
	return catalog.dealIndex.Len()
}

// LookupDeal returns one deal by deal_id for bid-path targeting.
func (catalog *RtbCatalog) LookupDeal(dealID string) (rtb.DealData, bool) {
	if catalog.dealIndex == nil {
		return rtb.DealData{}, false
	}
	return catalog.dealIndex.Lookup(dealID)
}

// AllDeals returns a snapshot of indexed PMP deals.
func (catalog *RtbCatalog) AllDeals() []rtb.DealData {
	if catalog.dealIndex == nil {
		return nil
	}
	return catalog.dealIndex.All()
}

// RunAuction runs an in-process auction for one ingest event.
// BudgetAuthorityShadow evaluates winners without debiting rtb or Redis budgets.
func (catalog *RtbCatalog) RunAuction(evt *campaignmodel.Event, targeting RtbTargetingInput) (rtb.AuctionResult, rtb.NoBidReason) {
	if catalog.authority != BudgetAuthorityShadow {
		if reason := rtbPrefilterReject(catalog.settingsWatcher, catalog, targeting); reason != rtb.NoBidNone {
			return rtb.AuctionResult{}, reason
		}
		if catalog.prebidIVT.Load() {
			if reason := rtbPrebidIVTReject(true, catalog.ingestGeo, evt); reason != rtb.NoBidNone {
				return rtb.AuctionResult{}, reason
			}
		}
		if targeting.SchainCount > 0 {
			allow := catalog.schainAllow.Load()
			if allow != nil && !ValidateSchainNodes(targeting.Schain, allow) {
				return rtb.AuctionResult{}, rtb.NoBidSchainInvalid
			}
		}
	}
	targeting = catalog.enrichTargetingDeal(targeting)
	req := BidRequestFromEvent(evt, targeting)
	if catalog.authority == BudgetAuthorityShadow {
		return catalog.registry.RunAuctionEval(&req)
	}
	res, reason := catalog.registry.RunAuction(&req)
	if reason.OK() && evt != nil {
		evt.ClearingPriceMicro = res.Price
	}
	return res, reason
}

func (catalog *RtbCatalog) enrichTargetingDeal(targeting RtbTargetingInput) RtbTargetingInput {
	if catalog == nil || catalog.dealIndex == nil {
		return targeting
	}
	var deal rtb.DealData
	var ok bool
	if targeting.DealIDLen > 0 {
		deal, ok = catalog.dealIndex.LookupBytes(targeting.DealIDBuf[:targeting.DealIDLen])
	} else if targeting.DealID != "" {
		deal, ok = catalog.LookupDeal(targeting.DealID)
	}
	if !ok {
		return targeting
	}
	if deal.PacingOpen == rtb.PacingClosed {
		targeting.DealBlock = rtb.NoBidPacingClosed
		return targeting
	}
	geoBit := rtb.GeoBitFromHash(targeting.GeoHash)
	if (deal.GeoMask&geoBit) == 0 || (deal.CatMask&targeting.CategoryMask) == 0 {
		targeting.DealBlock = rtb.NoBidDealMismatch
		return targeting
	}
	if deal.Seats > 0 && int32(targeting.SeatCount) < deal.Seats {
		targeting.DealBlock = rtb.NoBidDealMismatch
		return targeting
	}
	return targeting
}

// LookupDealBytes resolves a PMP deal by fixed buffer without heap allocation.
func (catalog *RtbCatalog) LookupDealBytes(dealID []byte) (rtb.DealData, bool) {
	if catalog == nil || catalog.dealIndex == nil {
		return rtb.DealData{}, false
	}
	return catalog.dealIndex.LookupBytes(dealID)
}

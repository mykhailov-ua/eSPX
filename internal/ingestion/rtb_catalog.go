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
	req := BidRequestFromEvent(evt, targeting)
	if catalog.authority == BudgetAuthorityShadow {
		return catalog.registry.RunAuctionEval(&req)
	}
	return catalog.registry.RunAuction(&req)
}

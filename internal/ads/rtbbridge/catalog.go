package rtbbridge

import (
	"sync/atomic"

	adcatalog "espx/internal/ads/catalog"
	"espx/internal/domain"
	"espx/internal/rtb"

	"github.com/google/uuid"
)

// RtbCatalog wires the in-process rtb registry to ads campaign sync and ingest targeting.
type RtbCatalog struct {
	registry   *rtb.Registry
	authority  BudgetAuthority
	winnerUUID atomic.Pointer[map[rtb.CampaignID]uuid.UUID]
}

// NewRtbCatalog creates an RTB catalog with the configured budget authority policy.
func NewRtbCatalog(store *rtb.BudgetStore, authority BudgetAuthority) *RtbCatalog {
	return &RtbCatalog{
		registry:  rtb.NewRegistry(store),
		authority: authority,
	}
}

// Registry exposes the underlying rtb registry for snapshots and budget admin paths.
func (catalog *RtbCatalog) Registry() *rtb.Registry {
	return catalog.registry
}

// Authority returns the configured budget ownership policy for this catalog.
func (catalog *RtbCatalog) Authority() BudgetAuthority {
	return catalog.authority
}

// SyncActiveCampaigns rebuilds the rtb catalog from active domain campaigns and auction inputs.
func (catalog *RtbCatalog) SyncActiveCampaigns(campaigns []*domain.Campaign, inputs map[uuid.UUID]RtbCampaignInput) {
	rows := BuildRtbCatalogRows(campaigns, inputs)
	catalog.registry.UpdateCampaigns(rows)
	catalog.rebuildWinnerUUID(rows, campaigns)
}

func (catalog *RtbCatalog) rebuildWinnerUUID(rows []rtb.CampaignData, campaigns []*domain.Campaign) {
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

// SyncCampaignRows publishes pre-built catalog rows and winner UUID mapping.
func (catalog *RtbCatalog) SyncCampaignRows(campaigns []*domain.Campaign, rows []rtb.CampaignData) {
	catalog.registry.UpdateCampaigns(rows)
	catalog.rebuildWinnerUUID(rows, campaigns)
}

// SyncFromRegistry reloads the rtb catalog from an ads CampaignRegistry snapshot.
func (c *RtbCatalog) SyncFromRegistry(registry *adcatalog.Registry, inputs map[uuid.UUID]RtbCampaignInput) {
	if registry == nil {
		c.registry.UpdateCampaigns(nil)
		return
	}
	c.SyncActiveCampaigns(registry.ActiveCampaigns(), inputs)
}

// SetClearingMode forwards clearing policy to the underlying rtb registry.
func (catalog *RtbCatalog) SetClearingMode(mode rtb.ClearingMode) {
	catalog.registry.SetClearingMode(mode)
}

// RunAuction executes an in-process auction for one ingest event.
// BudgetAuthorityShadow evaluates winners without debiting rtb or Redis budgets.
func (catalog *RtbCatalog) RunAuction(evt *domain.Event, targeting RtbTargetingInput) (rtb.AuctionResult, rtb.NoBidReason) {
	req := BidRequestFromEvent(evt, targeting)
	if catalog.authority == BudgetAuthorityShadow {
		return catalog.registry.RunAuctionEval(&req)
	}
	return catalog.registry.RunAuction(&req)
}

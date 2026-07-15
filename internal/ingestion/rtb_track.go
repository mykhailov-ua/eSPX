package ingestion

import (
	"espx/internal/campaignmodel"
	"espx/internal/config"
)

const (
	rtbModeOff uint8 = iota
	rtbModeShadow
	rtbModeLive
)

func rtbModeFromConfig(cfg *config.Config) uint8 {
	if cfg == nil {
		return rtbModeOff
	}
	switch config.ParseRtbMode(cfg.RtbMode) {
	case config.RtbModeShadow:
		return rtbModeShadow
	case config.RtbModeLive:
		return rtbModeLive
	default:
		return rtbModeOff
	}
}

// ConfigureTrackRtb wires RTB auction state into the shared track processor and Lua filter.
func ConfigureTrackRtb(proc *trackProcessor, cfg *config.Config, catalog *RtbCatalog, geo GeoProvider, unified *UnifiedFilter, watcher *SettingsWatcher) {
	if proc == nil || cfg == nil || catalog == nil || !cfg.RtbEnabled() {
		return
	}
	proc.rtbCatalog = catalog
	proc.rtbMode = rtbModeFromConfig(cfg)
	proc.ingestGeo = geo
	if unified != nil {
		setting := ""
		if watcher != nil {
			setting = watcher.Get().RtbBudgetAuthority
		}
		unified.SetSkipBudgetDebit(RtbSkipLuaBudgetDebit(cfg, setting))
	}
}

// ConfigureIngestGeo wires the shared GeoIP provider used for ingest geo deduplication.
func ConfigureIngestGeo(proc *trackProcessor, geo GeoProvider) {
	if proc != nil {
		proc.ingestGeo = geo
	}
}

// ConfigureHandlerRtb wires RTB into a gnet AdsPacketHandler.
func (h *AdsPacketHandler) ConfigureRtb(catalog *RtbCatalog, geo GeoProvider, unified *UnifiedFilter, watcher *SettingsWatcher) {
	if h == nil {
		return
	}
	ConfigureTrackRtb(&h.trackProc, h.cfg, catalog, geo, unified, watcher)
}

// ConfigureHandlerIngestGeo wires shared GeoIP lookup for ingest geo deduplication.
func (h *AdsPacketHandler) ConfigureIngestGeo(geo GeoProvider) {
	if h == nil {
		return
	}
	ConfigureIngestGeo(&h.trackProc, geo)
}

func buildRtbTargeting(evt *campaignmodel.Event, deviceType []byte, floorMicro int64, catalog *RtbCatalog) RtbTargetingInput {
	geoHash := uint32(0)
	dealID := ""
	if evt != nil && evt.IngestGeoResolved {
		geoHash = evt.GeoHash
	}
	if evt != nil && len(evt.Payload) > 0 {
		dealID = ParseDealID(evt.Payload)
	}

	// Try OpenRTB 3.0 parsing first
	if evt != nil && len(evt.Payload) > 0 {
		if minBid, devType, catMask, isOpenRTB := ParseOpenRTB3Payload(evt.Payload); isOpenRTB {
			if floorMicro <= 0 {
				floorMicro = minBid
			}
			floorMicro = EffectiveDealFloor(catalog, catalogDealFloors(catalog), dealID, floorMicro)
			return RtbTargetingInput{
				GeoHash:             geoHash,
				DeviceType:          devType,
				CategoryMask:        catMask,
				PublisherFloorMicro: floorMicro,
				DealID:              dealID,
			}
		}
	}

	// Fallback to legacy flat JSON parsing
	if floorMicro <= 0 && evt != nil {
		floorMicro = parseBidMicro(evt.Payload)
	}
	categoryMask := uint64(1)
	if evt != nil {
		if parsed := parseCategoryMask(evt.Payload); parsed != 0 {
			categoryMask = parsed
		}
	}
	floorMicro = EffectiveDealFloor(catalog, catalogDealFloors(catalog), dealID, floorMicro)
	return RtbTargetingInput{
		GeoHash:             geoHash,
		DeviceType:          DeviceMaskFromType(deviceType),
		CategoryMask:        categoryMask,
		PublisherFloorMicro: floorMicro,
		DealID:              dealID,
	}
}

func catalogDealFloors(catalog *RtbCatalog) *DealFloorCache {
	if catalog == nil {
		return nil
	}
	return catalog.dealFloors
}

func applyRtbAuction(proc trackProcessor, evt *campaignmodel.Event, deviceType []byte) (trackOutcome, bool) {
	if proc.rtbCatalog == nil || proc.rtbMode == rtbModeOff || evt == nil {
		return trackOutcome{}, false
	}

	targeting := buildRtbTargeting(evt, deviceType, 0, proc.rtbCatalog)
	payloadBidMicro := targeting.PublisherFloorMicro
	res, reason := proc.rtbCatalog.RunAuction(evt, targeting)

	if proc.rtbMode == rtbModeShadow {
		recordRtbShadowAuction(proc.rtbCatalog, evt, res, reason, payloadBidMicro)
		return trackOutcome{}, false
	}

	if !reason.OK() {
		return trackOutcome{Status: trackStatusRejected, RejectKind: noBidToRejectKind(reason)}, true
	}

	uid, ok := proc.rtbCatalog.UUIDForWinner(res.CampaignID)
	if !ok {
		return trackOutcome{Status: trackStatusRejected, RejectKind: filterRejectCampaignNotFound}, true
	}
	evt.CampaignID = uid
	return trackOutcome{}, false
}

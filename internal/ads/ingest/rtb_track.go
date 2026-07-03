package ingest

import (
	"espx/internal/ads/filter"
	"espx/internal/ads/rtbbridge"
	"espx/internal/config"
	"espx/internal/domain"
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
func ConfigureTrackRtb(proc *trackProcessor, cfg *config.Config, catalog *rtbbridge.RtbCatalog, geo filter.GeoProvider, unified *filter.UnifiedFilter) {
	if proc == nil || cfg == nil || catalog == nil || !cfg.RtbEnabled() {
		return
	}
	proc.rtbCatalog = catalog
	proc.rtbMode = rtbModeFromConfig(cfg)
	proc.ingestGeo = geo
	if unified != nil && cfg.RtbBudgetAuthoritative() {
		unified.SetSkipBudgetDebit(true)
	}
}

// ConfigureIngestGeo wires the shared GeoIP provider used for ingest geo deduplication.
func ConfigureIngestGeo(proc *trackProcessor, geo filter.GeoProvider) {
	if proc != nil {
		proc.ingestGeo = geo
	}
}

// ConfigureHandlerRtb wires RTB into a gnet AdsPacketHandler.
func (h *AdsPacketHandler) ConfigureRtb(catalog *rtbbridge.RtbCatalog, geo filter.GeoProvider, unified *filter.UnifiedFilter) {
	if h == nil {
		return
	}
	ConfigureTrackRtb(&h.trackProc, h.cfg, catalog, geo, unified)
}

// ConfigureHandlerIngestGeo wires shared GeoIP lookup for ingest geo deduplication.
func (h *AdsPacketHandler) ConfigureIngestGeo(geo filter.GeoProvider) {
	if h == nil {
		return
	}
	ConfigureIngestGeo(&h.trackProc, geo)
}

func buildRtbTargeting(evt *domain.Event, deviceType []byte, floorMicro int64) rtbbridge.RtbTargetingInput {
	geoHash := uint32(0)
	if evt != nil && evt.IngestGeoResolved {
		geoHash = evt.GeoHash
	}

	if evt != nil && len(evt.Payload) > 0 {
		if minBid, devType, catMask, isOpenRTB := rtbbridge.ParseOpenRTB3Payload(evt.Payload); isOpenRTB {
			if floorMicro <= 0 {
				floorMicro = minBid
			}
			return rtbbridge.RtbTargetingInput{
				GeoHash:             geoHash,
				DeviceType:          devType,
				CategoryMask:        catMask,
				PublisherFloorMicro: floorMicro,
			}
		}
	}

	if floorMicro <= 0 && evt != nil {
		floorMicro = filter.ParseBidMicro(evt.Payload)
	}
	categoryMask := uint64(1)
	if evt != nil {
		if parsed := filter.ParseCategoryMask(evt.Payload); parsed != 0 {
			categoryMask = parsed
		}
	}
	return rtbbridge.RtbTargetingInput{
		GeoHash:             geoHash,
		DeviceType:          rtbbridge.DeviceMaskFromType(deviceType),
		CategoryMask:        categoryMask,
		PublisherFloorMicro: floorMicro,
	}
}

func applyRtbAuction(proc trackProcessor, evt *domain.Event, deviceType []byte) (trackOutcome, bool) {
	if proc.rtbCatalog == nil || proc.rtbMode == rtbModeOff || evt == nil {
		return trackOutcome{}, false
	}

	targeting := buildRtbTargeting(evt, deviceType, 0)
	payloadBidMicro := targeting.PublisherFloorMicro
	res, reason := proc.rtbCatalog.RunAuction(evt, targeting)

	if proc.rtbMode == rtbModeShadow {
		rtbbridge.RecordRtbShadowAuction(proc.rtbCatalog, evt, res, reason, payloadBidMicro)
		return trackOutcome{}, false
	}

	if !reason.OK() {
		return trackOutcome{Status: trackStatusRejected, RejectKind: rtbbridge.NoBidToRejectKind(reason)}, true
	}

	uid, ok := proc.rtbCatalog.UUIDForWinner(res.CampaignID)
	if !ok {
		return trackOutcome{Status: trackStatusRejected, RejectKind: filter.FilterRejectCampaignNotFound}, true
	}
	evt.CampaignID = uid
	return trackOutcome{}, false
}

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
	proc.settingsWatcher = watcher
	if watcher != nil {
		proc.rtbMode = RtbModeFromSetting(watcher.Get().RtbMode, cfg)
		watcher.AddChangeListener(func(dc *DynamicConfig) {
			if proc != nil {
				proc.rtbMode = RtbModeFromSetting(dc.RtbMode, cfg)
			}
		})
	}
	proc.ingestGeo = geo
	catalog.ConfigureRtbGates(watcher, geo)
	if cfg.RtbPrebidIVTEnabled() {
		catalog.SetPrebidIVT(true)
	}
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
	if evt != nil && evt.IngestGeoResolved {
		geoHash = evt.GeoHash
	}

	out := RtbTargetingInput{GeoHash: geoHash}

	// Try OpenRTB 3.0 FSM first (shared with ingress / M12-02).
	if evt != nil && len(evt.Payload) > 0 {
		var parsed OpenRTB3Parsed
		var haveParsed bool
		if cached, ok := openRTB3ParsedFromScratch(evt); ok {
			parsed = *cached
			haveParsed = true
		} else {
			parsed = parseOpenRTB3FSM(evt.Payload)
			haveParsed = parsed.IsOpenRTB
		}
		if haveParsed {
			if floorMicro <= 0 {
				floorMicro = parsed.MinBid
			}
			if parsed.DealIDLen > 0 {
				out.DealIDLen = parsed.DealIDLen
				src := ortbSlice(evt.Payload, parsed.DealIDOff, parsed.DealIDLen)
				copy(out.DealIDBuf[:], src)
			}
			dealStr := ""
			if out.DealIDLen > 0 {
				dealStr = UnsafeString(out.DealIDBuf[:out.DealIDLen])
			}
			floorMicro = EffectiveDealFloor(catalog, catalogDealFloors(catalog), dealStr, floorMicro)
			out.DeviceType = parsed.DeviceType
			out.CategoryMask = parsed.CategoryMask
			out.PublisherFloorMicro = floorMicro
			return out
		}
	}

	// Legacy flat bid_micro / category_mask (deprecated; counted for one-release sunset).
	if evt != nil && len(evt.Payload) > 0 {
		incIngressLegacyJSON()
	}
	if floorMicro <= 0 && evt != nil {
		floorMicro = parseBidMicro(evt.Payload)
	}
	categoryMask := uint64(1)
	if evt != nil {
		if parsed := parseCategoryMask(evt.Payload); parsed != 0 {
			categoryMask = parsed
		}
		if n := ParseDealIDBytes(evt.Payload, out.DealIDBuf[:]); n > 0 {
			out.DealIDLen = uint8(n)
		}
	}
	dealStr := ""
	if out.DealIDLen > 0 {
		dealStr = UnsafeString(out.DealIDBuf[:out.DealIDLen])
	}
	floorMicro = EffectiveDealFloor(catalog, catalogDealFloors(catalog), dealStr, floorMicro)
	out.DeviceType = DeviceMaskFromType(deviceType)
	out.CategoryMask = categoryMask
	out.PublisherFloorMicro = floorMicro
	return out
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
		recordRtbDealOutcomeBytes(targeting.DealIDBuf[:], targeting.DealIDLen, payloadBidMicro, res, reason)
		return trackOutcome{}, false
	}

	recordRtbDealOutcomeBytes(targeting.DealIDBuf[:], targeting.DealIDLen, payloadBidMicro, res, reason)

	if !reason.OK() {
		return trackOutcome{Status: trackStatusRejected, RejectKind: noBidToRejectKind(reason)}, true
	}

	uid, ok := proc.rtbCatalog.UUIDForWinner(res.CampaignID)
	if !ok {
		return trackOutcome{Status: trackStatusRejected, RejectKind: filterRejectCampaignNotFound}, true
	}
	evt.CampaignID = uid
	evt.ClearingPriceMicro = res.Price
	return trackOutcome{}, false
}

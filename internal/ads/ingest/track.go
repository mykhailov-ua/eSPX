package ingest

import (
	"context"

	"espx/internal/ads/filter"
	"espx/internal/ads/rtbbridge"
	"espx/internal/domain"
)

// trackStatus is the shared /track decision after filters and landing resolution.
type trackStatus uint8

const (
	trackStatusAccepted trackStatus = iota
	trackStatusFraudAccepted
	trackStatusRejected
	trackStatusInternalError
)

// trackOutcome is the transport-agnostic result of processTrack.
type trackOutcome struct {
	Status     trackStatus
	RejectKind filter.FilterRejectKind
	LandingURL string
}

// trackProcessor holds dependencies shared by HTTP and gnet /track handlers.
type trackProcessor struct {
	filterEngine  *filter.FilterEngine
	registry      domain.CampaignRegistry
	creativeStore *filter.BrandCreativeStore
	rtbCatalog    *rtbbridge.RtbCatalog
	rtbMode       uint8
	ingestGeo     filter.GeoProvider
}

func newTrackProcessor(filterEngine *filter.FilterEngine, registry domain.CampaignRegistry, creativeStore *filter.BrandCreativeStore) trackProcessor {
	if filterEngine != nil {
		filterEngine.SetRegistry(registry)
	}
	return trackProcessor{
		filterEngine:  filterEngine,
		registry:      registry,
		creativeStore: creativeStore,
	}
}

// processTrack runs RTB (when configured), filter checks, and landing URL resolution for both ingest paths.
func processTrack(p trackProcessor, evt *domain.Event, deviceType []byte) trackOutcome {
	filter.EnsureIngestGeo(p.ingestGeo, evt)
	if out, handled := applyRtbAuction(p, evt, deviceType); handled {
		return out
	}
	if p.filterEngine != nil {
		if err := p.filterEngine.Check(context.Background(), evt); err != nil {
			if kind, ok := filter.ClassifyFilterErr(err); ok {
				if kind == filter.FilterRejectFraud {
					return trackOutcome{Status: trackStatusFraudAccepted, RejectKind: kind}
				}
				return trackOutcome{Status: trackStatusRejected, RejectKind: kind}
			}
			filter.FilterEngineFailures.Inc()
			return trackOutcome{Status: trackStatusInternalError}
		}
	}
	landing := filter.ResolveLandingURL(p.registry, p.creativeStore, evt)
	return trackOutcome{Status: trackStatusAccepted, LandingURL: landing}
}

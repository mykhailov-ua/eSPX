package ads

import (
	"context"

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
	RejectKind filterRejectKind
	LandingURL string
}

// trackProcessor holds dependencies shared by HTTP and gnet /track handlers.
type trackProcessor struct {
	filterEngine  *FilterEngine
	registry      domain.CampaignRegistry
	creativeStore *BrandCreativeStore
	rtbCatalog    *RtbCatalog
	rtbMode       uint8
	ingestGeo     GeoProvider
}

func newTrackProcessor(filterEngine *FilterEngine, registry domain.CampaignRegistry, creativeStore *BrandCreativeStore) trackProcessor {
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
	ensureIngestGeo(p.ingestGeo, evt)
	if out, handled := applyRtbAuction(p, evt, deviceType); handled {
		return out
	}
	if p.filterEngine != nil {
		if err := p.filterEngine.Check(context.Background(), evt); err != nil {
			if kind, ok := classifyFilterErr(err); ok {
				if kind == filterRejectFraud {
					return trackOutcome{Status: trackStatusFraudAccepted, RejectKind: kind}
				}
				return trackOutcome{Status: trackStatusRejected, RejectKind: kind}
			}
			filterEngineFailures.Inc()
			return trackOutcome{Status: trackStatusInternalError}
		}
	}
	landing := ResolveLandingURL(p.registry, p.creativeStore, evt)
	return trackOutcome{Status: trackStatusAccepted, LandingURL: landing}
}

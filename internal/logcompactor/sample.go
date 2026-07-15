package logcompactor

import (
	"hash/fnv"

	"espx/internal/ingestion/pb"
)

// isAlwaysKeepEvent returns true for billable and fraud-marked audit records.
func isAlwaysKeepEvent(evt *pb.AdStreamEvent) bool {
	if evt == nil {
		return false
	}
	switch string(evt.EventType) {
	case "click", "conversion":
		return true
	}
	return evt.GhostEvent || evt.FraudScore > 0
}

// shouldSampleImpression applies deterministic hash downsampling to impression records.
func shouldSampleImpression(clickID []byte, sampleRate uint64) bool {
	if len(clickID) == 0 {
		return false
	}
	if sampleRate == 0 {
		sampleRate = 1
	}
	h := fnv.New64a()
	_, _ = h.Write(clickID)
	return h.Sum64()%sampleRate == 0
}

// shouldKeepEvent decides whether an audit record survives warm-tier downsampling.
// Deprecated path for tests comparing legacy hash-only sampling.
func shouldKeepEvent(evt *pb.AdStreamEvent, sampleRate uint64) bool {
	if isAlwaysKeepEvent(evt) {
		return true
	}
	return shouldSampleImpression(evt.ClickId, sampleRate)
}

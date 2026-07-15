package ingestion

import (
	"sync/atomic"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/ingestion/pb"
	"espx/internal/metrics"
	"espx/pkg/logger"

	"github.com/google/uuid"
)

// auditLogSampleMaskDefault inherits the Lua metrics downsampling rate for audit logs.
const auditLogSampleMaskDefault = luaMetricsSampleMask

// auditLogSampleMaskFromConfig aligns audit downsampling with Lua histogram sampling from ops config.
func auditLogSampleMaskFromConfig(cfgVal int) uint64 {
	return histogramSampleMaskFromConfig(cfgVal)
}

// auditLogPriority assigns higher logger priority to billable events during disk pressure.
func auditLogPriority(eventType string) uint8 {
	switch eventType {
	case "click", "conversion":
		return 1
	default:
		return 0
	}
}

// writeAuditLog emits a sampled AdStreamEvent protobuf for accepted track events (broker ingest path).
func writeAuditLog(
	l *logger.Logger,
	seq *atomic.Uint64,
	sampleMask uint64,
	shardID int,
	evt *campaignmodel.Event,
) {
	if l == nil || evt == nil {
		return
	}
	priority := auditLogPriority(evt.Type)
	if priority == 0 {
		if !shouldSampleHistogram(seq.Add(1), sampleMask) {
			return
		}
	}

	createdAt := evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = CachedTimeUTC()
	}

	pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
	pbEvt.ClickId = UnsafeBytes(evt.ClickID)
	pbEvt.CampaignId = evt.CampaignID[:]
	pbEvt.EventType = UnsafeBytes(evt.Type)
	pbEvt.Payload = evt.Payload
	pbEvt.Ip = UnsafeBytes(evt.IP)
	pbEvt.Ua = UnsafeBytes(evt.UA)
	pbEvt.UserId = UnsafeBytes(evt.UserID)
	pbEvt.CreatedAtUnix = createdAt.Unix()
	pbEvt.FraudScore = evt.FraudScore
	pbEvt.FraudReason = UnsafeBytes(evt.FraudReason)
	pbEvt.GhostEvent = evt.GhostEvent

	size := pbEvt.SizeVT()
	bufPtr := logBufPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	n, err := pbEvt.MarshalToSizedBufferVT(buf)
	if err == nil {
		if !l.WriteToShard(shardID, priority, buf[:n]) {
			metrics.HandlerLogDropTotal.Inc()
		}
	}
	*bufPtr = buf
	logBufPool.Put(bufPtr)

	ClearAdStreamEvent(pbEvt)
	streamEventPool.Put(pbEvt)
}

// auditEventFromFields builds a minimal domain event for cold-path audit writes.
func auditEventFromFields(ts int64, campaignID uuid.UUID, clickID, eventType string) *campaignmodel.Event {
	evt := campaignmodel.EventPool.Get().(*campaignmodel.Event)
	evt.Reset()
	evt.ClickID = clickID
	evt.CampaignID = campaignID
	evt.Type = eventType
	if ts > 0 {
		evt.CreatedAt = time.Unix(ts, 0)
	}
	return evt
}

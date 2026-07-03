package processor

import (
	"sync/atomic"
	"time"

	"espx/internal/ads/clock"
	"espx/internal/ads/filter"
	"espx/internal/ads/pb"
	"espx/internal/ads/repo"
	"espx/internal/domain"
	"espx/internal/metrics"
	"espx/pkg/logger"

	"github.com/google/uuid"
)

// AuditLogSampleMaskDefault inherits the Lua metrics downsampling rate for audit logs.
const AuditLogSampleMaskDefault = filter.LuaMetricsSampleMask

// AuditLogSampleMaskFromConfig aligns audit downsampling with Lua histogram sampling from ops config.
func AuditLogSampleMaskFromConfig(cfgVal int) uint64 {
	return filter.HistogramSampleMaskFromConfig(cfgVal)
}

func auditLogSampleMaskFromConfig(cfgVal int) uint64 {
	return AuditLogSampleMaskFromConfig(cfgVal)
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

// WriteAuditLog emits a sampled AdStreamEvent protobuf for accepted track events (broker ingest path).
func WriteAuditLog(
	l *logger.Logger,
	seq *atomic.Uint64,
	sampleMask uint64,
	shardID int,
	evt *domain.Event,
) {
	if l == nil || evt == nil {
		return
	}
	priority := auditLogPriority(evt.Type)
	if priority == 0 {
		if !filter.ShouldSampleHistogram(seq.Add(1), sampleMask) {
			return
		}
	}

	createdAt := evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = clock.CachedTimeUTC()
	}

	pbEvt := repo.StreamEventPool.Get().(*pb.AdStreamEvent)
	pbEvt.ClickId = repo.UnsafeBytes(evt.ClickID)
	pbEvt.CampaignId = evt.CampaignID[:]
	pbEvt.EventType = repo.UnsafeBytes(evt.Type)
	pbEvt.Payload = evt.Payload
	pbEvt.Ip = repo.UnsafeBytes(evt.IP)
	pbEvt.Ua = repo.UnsafeBytes(evt.UA)
	pbEvt.UserId = repo.UnsafeBytes(evt.UserID)
	pbEvt.CreatedAtUnix = createdAt.Unix()
	pbEvt.FraudScore = evt.FraudScore
	pbEvt.FraudReason = repo.UnsafeBytes(evt.FraudReason)
	pbEvt.GhostEvent = evt.GhostEvent

	size := pbEvt.SizeVT()
	bufPtr := repo.ByteBufPool.Get().(*[]byte)
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
	repo.ByteBufPool.Put(bufPtr)

	repo.ClearAdStreamEvent(pbEvt)
	repo.StreamEventPool.Put(pbEvt)
}

// auditEventFromFields builds a minimal domain event for cold-path audit writes.
func auditEventFromFields(ts int64, campaignID uuid.UUID, clickID, eventType string) *domain.Event {
	evt := domain.EventPool.Get().(*domain.Event)
	evt.Reset()
	evt.ClickID = clickID
	evt.CampaignID = campaignID
	evt.Type = eventType
	if ts > 0 {
		evt.CreatedAt = time.Unix(ts, 0)
	}
	return evt
}

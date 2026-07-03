package broker

import (
	"time"

	"espx/internal/ads/pb"
	"espx/internal/ads/repo"
	"espx/internal/domain"

	"github.com/google/uuid"
)

// ParseBrokerPayload decodes broker log bytes as AdStreamEvent or AdLogRecord into a pooled Event.
func ParseBrokerPayload(data []byte) (*domain.Event, error) {
	evt := domain.EventPool.Get().(*domain.Event)
	evt.Reset()

	pbEvt := repo.StreamEventPool.Get().(*pb.AdStreamEvent)
	repo.DeepResetAdStreamEvent(pbEvt)
	if err := pbEvt.UnmarshalVT(data); err == nil {
		fillEventFromStreamProto(pbEvt, evt)
		repo.DeepResetAdStreamEvent(pbEvt)
		repo.StreamEventPool.Put(pbEvt)
		return evt, nil
	}
	repo.DeepResetAdStreamEvent(pbEvt)
	repo.StreamEventPool.Put(pbEvt)

	rec := repo.AdLogRecordPool.Get().(*pb.AdLogRecord)
	rec.Reset()
	if err := rec.UnmarshalVT(data); err == nil {
		fillEventFromLogRecord(rec, evt)
		campIDSaved := rec.CampaignId
		rec.Reset()
		if cap(campIDSaved) >= 16 {
			rec.CampaignId = campIDSaved[:0]
		}
		repo.AdLogRecordPool.Put(rec)
		return evt, nil
	}
	campIDSaved := rec.CampaignId
	rec.Reset()
	if cap(campIDSaved) >= 16 {
		rec.CampaignId = campIDSaved[:0]
	}
	repo.AdLogRecordPool.Put(rec)
	domain.EventPool.Put(evt)
	return nil, ErrBrokerPayloadUnrecognized
}

// fillEventFromStreamProto maps a stream protobuf into a pooled domain event.
func fillEventFromStreamProto(pbEvt *pb.AdStreamEvent, evt *domain.Event) {
	totalLen := len(pbEvt.ClickId) + len(pbEvt.EventType) + len(pbEvt.Ip) + len(pbEvt.Ua) + len(pbEvt.FraudReason)
	if cap(evt.StringBuffer) < totalLen {
		evt.StringBuffer = make([]byte, 0, totalLen+128)
	} else {
		evt.StringBuffer = evt.StringBuffer[:0]
	}

	evt.StringBuffer = append(evt.StringBuffer, pbEvt.ClickId...)
	evt.ClickID = repo.UnsafeString(evt.StringBuffer[len(evt.StringBuffer)-len(pbEvt.ClickId):])

	evt.StringBuffer = append(evt.StringBuffer, pbEvt.EventType...)
	evt.Type = repo.UnsafeString(evt.StringBuffer[len(evt.StringBuffer)-len(pbEvt.EventType):])

	evt.StringBuffer = append(evt.StringBuffer, pbEvt.Ip...)
	evt.IP = repo.UnsafeString(evt.StringBuffer[len(evt.StringBuffer)-len(pbEvt.Ip):])

	evt.StringBuffer = append(evt.StringBuffer, pbEvt.Ua...)
	evt.UA = repo.UnsafeString(evt.StringBuffer[len(evt.StringBuffer)-len(pbEvt.Ua):])

	if len(pbEvt.UserId) > 0 {
		evt.StringBuffer = append(evt.StringBuffer, pbEvt.UserId...)
		evt.UserID = repo.UnsafeString(evt.StringBuffer[len(evt.StringBuffer)-len(pbEvt.UserId):])
	}

	_ = repo.ParseUUID(pbEvt.CampaignId, &evt.CampaignID)
	evt.Payload = append(evt.Payload[:0], pbEvt.Payload...)
	if len(pbEvt.FraudReason) > 0 {
		evt.StringBuffer = append(evt.StringBuffer, pbEvt.FraudReason...)
		evt.FraudReason = repo.UnsafeString(evt.StringBuffer[len(evt.StringBuffer)-len(pbEvt.FraudReason):])
	}
	evt.FraudScore = pbEvt.FraudScore
	evt.GhostEvent = pbEvt.GhostEvent
	if pbEvt.CreatedAtUnix > 0 {
		evt.CreatedAt = time.Unix(pbEvt.CreatedAtUnix, 0)
	}
}

func fillEventFromLogRecord(rec *pb.AdLogRecord, evt *domain.Event) {
	if cap(evt.StringBuffer) < len(rec.ClickId)+len(rec.EventType) {
		evt.StringBuffer = make([]byte, 0, len(rec.ClickId)+len(rec.EventType)+64)
	} else {
		evt.StringBuffer = evt.StringBuffer[:0]
	}
	evt.StringBuffer = append(evt.StringBuffer, rec.ClickId...)
	evt.ClickID = repo.UnsafeString(evt.StringBuffer[len(evt.StringBuffer)-len(rec.ClickId):])
	evt.StringBuffer = append(evt.StringBuffer, rec.EventType...)
	evt.Type = repo.UnsafeString(evt.StringBuffer[len(evt.StringBuffer)-len(rec.EventType):])
	if len(rec.CampaignId) >= 16 {
		_ = repo.ParseUUID(rec.CampaignId[:16], &evt.CampaignID)
	} else {
		evt.CampaignID = uuid.Nil
	}
	if rec.TimestampUnix > 0 {
		evt.CreatedAt = time.Unix(rec.TimestampUnix, 0)
	}
}

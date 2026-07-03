package ads

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"

	"espx/internal/domain"
)

// trackIngestFields holds decoded /track POST fields before processTrack.
type trackIngestFields struct {
	campaignID uuid.UUID
	eventType  string
	userID     string
	payload    []byte
	clickID    string
	deviceType []byte
}

// parseTrackIngest decodes protobuf or JSON /track bodies into shared ingest fields.
func (h *AdsPacketHandler) parseTrackIngest(
	ctx *connContext,
	req parsedHTTPRequest,
	wReqID *bufWrapper,
) (fields trackIngestFields, badResp []byte, httpStatus int, ok bool) {
	contentType := unsafeString(req.ContentType)
	if contentType == "application/x-protobuf" || contentType == "" {
		h.trackMetrics.throughputProto.Inc()
		pbReq := &ctx.pbReq
		pbReq.CampaignId = pbReq.CampaignId[:0]
		pbReq.EventType = pbReq.EventType[:0]
		if pbReq.Metadata != nil {
			pbReq.Metadata.ClickId = pbReq.Metadata.ClickId[:0]
			pbReq.Metadata.UserId = pbReq.Metadata.UserId[:0]
			pbReq.Metadata.DeviceType = pbReq.Metadata.DeviceType[:0]
			pbReq.Metadata.Os = pbReq.Metadata.Os[:0]
			for i := range pbReq.Metadata.ExtraKeys {
				pbReq.Metadata.ExtraKeys[i] = pbReq.Metadata.ExtraKeys[i][:0]
			}
			pbReq.Metadata.ExtraKeys = pbReq.Metadata.ExtraKeys[:0]
			for i := range pbReq.Metadata.ExtraValues {
				pbReq.Metadata.ExtraValues[i] = pbReq.Metadata.ExtraValues[i][:0]
			}
			pbReq.Metadata.ExtraValues = pbReq.Metadata.ExtraValues[:0]
			pbReq.Metadata.ExtraBytes = pbReq.Metadata.ExtraBytes[:0]
		}

		if err := pbReq.UnmarshalVT(req.Body); err != nil {
			return fields, respInvalidProto, http.StatusBadRequest, false
		}

		if len(pbReq.CampaignId) != 16 {
			return fields, respInvalidCampaign, http.StatusBadRequest, false
		}
		copy(fields.campaignID[:], pbReq.CampaignId)

		fields.eventType = unsafeString(pbReq.EventType)
		if pbReq.Metadata != nil {
			fields.userID = unsafeString(pbReq.Metadata.UserId)
			if len(pbReq.Metadata.ClickId) > 0 {
				fields.clickID = unsafeString(pbReq.Metadata.ClickId)
			}
			if len(pbReq.Metadata.ExtraBytes) > 0 {
				fields.payload = pbReq.Metadata.ExtraBytes
			} else if len(pbReq.Metadata.ExtraKeys) > 0 {
				ctx.extraBuf = marshalExtra(ctx.extraBuf, pbReq.Metadata.ExtraKeys, pbReq.Metadata.ExtraValues)
				fields.payload = ctx.extraBuf
			}
			fields.deviceType = pbReq.Metadata.DeviceType
		}
		return fields, nil, 0, true
	}

	h.trackMetrics.throughputJSON.Inc()
	trackReq := &ctx.trackReq
	trackReq.Reset()

	if err := ParseTrackRequestJSON(trackReq, req.Body); err != nil {
		return fields, respInvalidJSON, http.StatusBadRequest, false
	}
	fields.campaignID = trackReq.CampaignID
	fields.userID = trackReq.UserID
	fields.eventType = trackReq.Type
	fields.payload = trackReq.Payload
	if trackReq.ClickID != "" {
		fields.clickID = trackReq.ClickID
	}
	return fields, nil, 0, true
}

func fillTrackEvent(evt *domain.Event, fields trackIngestFields, ip, ua string) {
	evt.Reset()
	evt.ClickID = fields.clickID
	evt.CampaignID = fields.campaignID
	evt.UserID = fields.userID
	evt.Type = fields.eventType
	evt.Payload = append(evt.Payload[:0], fields.payload...)
	evt.IP = ip
	evt.UA = ua
}

// deliverGnetTrack maps processTrack outcomes to gnet responses and metrics.
func (h *AdsPacketHandler) deliverGnetTrack(
	ctx *connContext,
	req parsedHTTPRequest,
	c gnet.Conn,
	evt *domain.Event,
	startMono int64,
	wReqID *bufWrapper,
	requestIDStr string,
	outcome trackOutcome,
) gnet.Action {
	switch outcome.Status {
	case trackStatusFraudAccepted:
		h.trackMetrics.recordFilterReject(outcome.RejectKind)
		shard := h.sharder.GetShard(evt.CampaignID)
		enqueueFraudReject(h.fraudWriter, shard, evt)
		h.writeGnetTrackAccepted(ctx, req, c, startMono, wReqID, requestIDStr, "")
		return gnet.None
	case trackStatusRejected:
		spec := filterRejectSpecs[outcome.RejectKind]
		h.trackMetrics.recordFilterReject(outcome.RejectKind)
		h.write(c, spec.gnetResp, ctx)
		h.recordMetrics(startMono, spec.status)
		return gnet.None
	case trackStatusInternalError:
		h.write(c, respInternalError, ctx)
		h.recordMetrics(startMono, http.StatusInternalServerError)
		return gnet.None
	case trackStatusAccepted:
		h.trackMetrics.decisionAccepted.Inc()
		writeAuditLog(h.logger, &h.auditLogSeq, h.auditLogSampleMask, ctx.shardID, evt)
		h.writeGnetTrackAccepted(ctx, req, c, startMono, wReqID, requestIDStr, outcome.LandingURL)
		return gnet.None
	default:
		h.write(c, respInternalError, ctx)
		h.recordMetrics(startMono, http.StatusInternalServerError)
		return gnet.None
	}
}

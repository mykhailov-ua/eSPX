package ingestion

// handler_http2.go — HTTP/2 cleartext (h2c) ingress on gnet (M5-C3).

import (
	"errors"

	"espx/internal/campaignmodel"
	"espx/internal/ingestion/pb"
	"espx/internal/metrics"

	"github.com/panjf2000/gnet/v2"
)

func (h *AdsPacketHandler) onTrafficH2(c gnet.Conn, buf []byte) gnet.Action {
	maxBody := int64(1 << 20)
	if h != nil && h.cfg != nil {
		maxBody = h.cfg.MaxRequestBodySize
	}
	incompleteMax := uint8(3)
	if h != nil && h.cfg != nil && h.cfg.H2IncompleteMax > 0 {
		if h.cfg.H2IncompleteMax > 255 {
			incompleteMax = 255
		} else {
			incompleteMax = uint8(h.cfg.H2IncompleteMax)
		}
	}

	ctx, ok := c.Context().(*connContext)
	if !ok || ctx == nil {
		ctx = h.allocConnContext(c)
		c.SetContext(ctx)
	}
	ctx.protoH2 = true

	consumed, req, streamID, settings, err := parseH2Ingress(buf, &ctx.h2, maxBody)
	if len(settings) > 0 {
		_, _ = c.Write(settings)
	}
	if consumed > 0 {
		ctx.h2.incompleteSpin = 0
		if _, derr := c.Discard(consumed); derr != nil {
			return gnet.Close
		}
	}
	if err != nil {
		if errors.Is(err, errIncompleteRequest) {
			if consumed == 0 {
				ctx.h2.incompleteSpin++
				if ctx.h2.incompleteSpin >= incompleteMax {
					metrics.H2HostileDisconnectTotal.Inc()
					return gnet.Close
				}
			}
			return gnet.None
		}
		if errors.Is(err, errPayloadTooLarge) {
			ctx.h2StreamID = streamID
			h.write(c, respPayloadTooLarge, ctx)
			return gnet.Close
		}
		ctx.h2StreamID = streamID
		h.write(c, respBadRequestClose, ctx)
		return gnet.Close
	}
	ctx.h2.incompleteSpin = 0
	if len(req.Method) == 0 {
		return gnet.None
	}
	ctx.h2StreamID = streamID
	act := h.React(req, c)
	ctx.h2StreamID = 0
	return act
}

func (h *AdsPacketHandler) allocConnContext(c gnet.Conn) *connContext {
	ctx := &connContext{
		pbReq: pb.AdEvent{
			Metadata: &pb.EventMetadata{},
		},
		trackReq: TrackRequest{
			Payload: make([]byte, 0, 512),
		},
		evt: campaignmodel.Event{
			Payload: make([]byte, 0, 1024),
		},
		valSlice: make([]any, 18),
		resp:     pb.TrackResponse{},
		bufSlice: make([]byte, 4096),
		h2:       newH2ConnState(),
		wReqID: bufWrapper{
			buf: make([]byte, 0, 128),
		},
		wCamp: bufWrapper{
			buf: make([]byte, 0, 128),
		},
		wTime: bufWrapper{
			buf: make([]byte, 0, 128),
		},
	}
	if h.logger != nil {
		ctx.shardID = int(h.loggerShardCounter.Add(1) % uint64(len(h.logger.Shards())))
	}
	return ctx
}

package ingestion

import (
	"github.com/panjf2000/gnet/v2"
)

func (h *AdsPacketHandler) reactOpenRTBBid(req parsedHTTPRequest, c gnet.Conn, ctx *connContext) gnet.Action {
	if h == nil {
		return gnet.None
	}
	bidID := ctx.wReqID.buf[:0]
	id := NewFastUUID()
	bidID = appendUUID(bidID, id)
	ctx.wReqID.buf = bidID

	outcome := runOpenRTBBid(h.trackProc, req.Body, bidID, extractClientIPGnet(ctx, &req, c, h.cfg.TrustedProxies))
	buf := ctx.bufSlice
	var n int
	if outcome.HasBid {
		n = writeOpenRTB26BidHTTP(buf, bidID, outcome.PriceMicro, outcome.CampaignID)
	} else {
		n = writeOpenRTB26NoBidHTTP(buf)
	}
	h.write(c, buf[:n], ctx)
	return gnet.None
}

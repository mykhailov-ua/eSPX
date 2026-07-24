package ingestion

import (
	"espx/internal/campaignmodel"
	"espx/internal/rtb"

	"github.com/google/uuid"
)

// writeOpenRTB26NoBidHTTP writes a 204 nobid HTTP response into buf.
func writeOpenRTB26NoBidHTTP(buf []byte) int {
	const body = `{"id":"","nbr":2}`
	n := copy(buf, "HTTP/1.1 204 No Content\r\nContent-Type: application/json\r\nContent-Length: ")
	n += appendInt(buf[n:], int64(len(body)))
	n += copy(buf[n:], "\r\nConnection: keep-alive\r\n\r\n")
	n += copy(buf[n:], body)
	return n
}

// writeOpenRTB26BidHTTP writes a 200 bid HTTP response into buf without heap allocation.
func writeOpenRTB26BidHTTP(buf []byte, bidID []byte, priceMicro int64, campaignID uuid.UUID) int {
	var body [256]byte
	j := 0
	j += copy(body[j:], `{"id":"`)
	j += copy(body[j:], bidID)
	j += copy(body[j:], `","seatbid":[{"bid":[{"id":"1","impid":"1","price":`)
	j += appendMicroDecimal(body[j:], priceMicro)
	j += copy(body[j:], `,"adid":"`)
	j += appendUUIDCompact(body[j:], campaignID)
	j += copy(body[j:], `","cid":"`)
	j += appendUUIDCompact(body[j:], campaignID)
	j += copy(body[j:], `"}]}]}`)

	n := 0
	n += copy(buf[n:], "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ")
	n += appendInt(buf[n:], int64(j))
	n += copy(buf[n:], "\r\nConnection: keep-alive\r\n\r\n")
	n += copy(buf[n:], body[:j])
	return n
}

func appendMicroDecimal(dst []byte, micro int64) int {
	if micro < 0 {
		micro = 0
	}
	whole := micro / 1_000_000
	frac := micro % 1_000_000
	n := appendInt(dst, whole)
	dst[n] = '.'
	n++
	dst[n+0] = byte('0' + frac/100000)
	frac %= 100000
	dst[n+1] = byte('0' + frac/10000)
	frac %= 10000
	dst[n+2] = byte('0' + frac/1000)
	frac %= 1000
	dst[n+3] = byte('0' + frac/100)
	frac %= 100
	dst[n+4] = byte('0' + frac/10)
	dst[n+5] = byte('0' + frac%10)
	return n + 6
}

func appendInt(dst []byte, v int64) int {
	if v == 0 {
		dst[0] = '0'
		return 1
	}
	if v < 0 {
		dst[0] = '-'
		return 1 + appendUint(dst[1:], uint64(-v))
	}
	return appendUint(dst, uint64(v))
}

func appendUint(dst []byte, v uint64) int {
	if v == 0 {
		dst[0] = '0'
		return 1
	}
	var tmp [20]byte
	i := len(tmp)
	for v > 0 {
		i--
		tmp[i] = byte('0' + v%10)
		v /= 10
	}
	return copy(dst, tmp[i:])
}

func appendUUIDCompact(dst []byte, id uuid.UUID) int {
	const hex = "0123456789abcdef"
	n := 0
	for i := 0; i < 16; i++ {
		b := id[i]
		dst[n] = hex[b>>4]
		dst[n+1] = hex[b&0x0f]
		n += 2
		if i == 3 || i == 5 || i == 7 || i == 9 {
			dst[n] = '-'
			n++
		}
	}
	return n
}

// OpenRTBBidOutcome is the transport-agnostic result of an OpenRTB auction on /openrtb/bid.
type OpenRTBBidOutcome struct {
	HasBid     bool
	PriceMicro int64
	CampaignID uuid.UUID
	NoBid      rtb.NoBidReason
}

// runOpenRTBBid executes buildRtbTargeting → RunAuction for a parsed OpenRTB 2.6 request.
func runOpenRTBBid(proc trackProcessor, body []byte, bidID []byte, clientIP string) OpenRTBBidOutcome {
	if proc.rtbCatalog == nil || proc.rtbMode == rtbModeOff {
		return OpenRTBBidOutcome{NoBid: rtb.NoBidInvalidRequest}
	}
	parsed := ParseOpenRTB26(body)
	if !parsed.OK {
		return OpenRTBBidOutcome{NoBid: rtb.NoBidInvalidRequest}
	}

	evt := &campaignmodel.Event{Payload: body, IP: clientIP}
	ensureIngestGeo(proc.ingestGeo, evt)

	targeting := RtbTargetingInput{
		GeoHash:             evt.GeoHash,
		DeviceType:          parsed.DeviceType,
		CategoryMask:        parsed.CategoryMask,
		PublisherFloorMicro: parsed.BidFloorMicro,
		SeatCount:           parsed.SeatCount,
		DeadlineMono:        DeadlineMonoFromTmax(parsed.TmaxMs),
	}
	if parsed.DealIDLen > 0 {
		targeting.DealIDLen = parsed.DealIDLen
		copy(targeting.DealIDBuf[:], parsed.DealID[:parsed.DealIDLen])
	}
	if parsed.Schain.Count > 0 {
		targeting.Schain = parsed.Schain
		targeting.SchainCount = parsed.Schain.Count
	}
	if parsed.BidFloorMicro <= 0 && parsed.DealIDLen > 0 {
		if deal, ok := proc.rtbCatalog.LookupDealBytes(parsed.DealID[:parsed.DealIDLen]); ok && deal.FloorMicro > 0 {
			targeting.PublisherFloorMicro = deal.FloorMicro
		}
	}

	res, reason := proc.rtbCatalog.RunAuction(evt, targeting)
	recordRtbDealOutcomeBytes(parsed.DealID[:parsed.DealIDLen], parsed.DealIDLen, parsed.BidFloorMicro, res, reason)

	if proc.rtbMode == rtbModeShadow {
		recordRtbShadowAuction(proc.rtbCatalog, evt, res, reason, targeting.PublisherFloorMicro)
		return OpenRTBBidOutcome{NoBid: reason}
	}
	if !reason.OK() {
		return OpenRTBBidOutcome{NoBid: reason}
	}
	uid, ok := proc.rtbCatalog.UUIDForWinner(res.CampaignID)
	if !ok {
		return OpenRTBBidOutcome{NoBid: rtb.NoBidNoCandidates}
	}
	return OpenRTBBidOutcome{
		HasBid:     true,
		PriceMicro: res.Price,
		CampaignID: uid,
	}
}

package ingestion

import (
	"hash/crc32"

	"espx/internal/ingestion/pb"
	"github.com/google/uuid"
)

// FormatUUIDCanonical writes a lowercase canonical UUID string into dst (36 bytes).
func FormatUUIDCanonical(dst *[36]byte, id uuid.UUID) {
	b := dst[:]
	b[0] = hexChars[id[0]>>4]
	b[1] = hexChars[id[0]&0xf]
	b[2] = hexChars[id[1]>>4]
	b[3] = hexChars[id[1]&0xf]
	b[4] = hexChars[id[2]>>4]
	b[5] = hexChars[id[2]&0xf]
	b[6] = hexChars[id[3]>>4]
	b[7] = hexChars[id[3]&0xf]
	b[8] = '-'
	b[9] = hexChars[id[4]>>4]
	b[10] = hexChars[id[4]&0xf]
	b[11] = hexChars[id[5]>>4]
	b[12] = hexChars[id[5]&0xf]
	b[13] = '-'
	b[14] = hexChars[id[6]>>4]
	b[15] = hexChars[id[6]&0xf]
	b[16] = hexChars[id[7]>>4]
	b[17] = hexChars[id[7]&0xf]
	b[18] = '-'
	b[19] = hexChars[id[8]>>4]
	b[20] = hexChars[id[8]&0xf]
	b[21] = hexChars[id[9]>>4]
	b[22] = hexChars[id[9]&0xf]
	b[23] = '-'
	b[24] = hexChars[id[10]>>4]
	b[25] = hexChars[id[10]&0xf]
	b[26] = hexChars[id[11]>>4]
	b[27] = hexChars[id[11]&0xf]
	b[28] = hexChars[id[12]>>4]
	b[29] = hexChars[id[12]&0xf]
	b[30] = hexChars[id[13]>>4]
	b[31] = hexChars[id[13]&0xf]
	b[32] = hexChars[id[14]>>4]
	b[33] = hexChars[id[14]&0xf]
	b[34] = hexChars[id[15]>>4]
	b[35] = hexChars[id[15]&0xf]
}

// ComputeCompositeHashUUID hashes campaign_id (canonical UUID text) + user_id bytes
// without allocating, matching nginx ngx.crc32_long(campaign_id .. user_id).
func ComputeCompositeHashUUID(campaignID uuid.UUID, userID []byte) uint32 {
	var crc uint32
	var started bool

	if campaignID != uuid.Nil {
		var buf [36]byte
		FormatUUIDCanonical(&buf, campaignID)
		crc = crc32IEEEInplace36(&buf)
		started = true
	}
	if len(userID) > 0 {
		if started {
			crc = crc32.Update(crc, crc32.IEEETable, userID)
		} else {
			crc = crc32.ChecksumIEEE(userID)
		}
	}
	if !started && len(userID) == 0 {
		return 0
	}
	return crc
}

// crc32IEEEInplace36 checksums a stack-allocated UUID text buffer without heap escape.
func crc32IEEEInplace36(b *[36]byte) uint32 {
	crc := ^uint32(0)
	tab := crc32.IEEETable
	for i := 0; i < 36; i++ {
		crc = tab[byte(crc)^b[i]] ^ crc>>8
	}
	return ^crc
}

// ComputeCompositeHashFromTrackReq hashes routing key from a parsed JSON track request.
func ComputeCompositeHashFromTrackReq(req *TrackRequest) uint32 {
	return ComputeCompositeHashUUID(req.CampaignID, UnsafeBytes(req.UserID))
}

// ComputeCompositeHashFromProto hashes routing key from a decoded protobuf AdEvent.
func ComputeCompositeHashFromProto(req *pb.AdEvent) uint32 {
	var camp uuid.UUID
	if len(req.CampaignId) == 16 {
		copy(camp[:], req.CampaignId)
	}
	var userID []byte
	if req.Metadata != nil {
		userID = req.Metadata.UserId
	}
	return ComputeCompositeHashUUID(camp, userID)
}

// resetAdEventInPlace clears slice fields before re-unmarshal while keeping Metadata allocated.
func resetAdEventInPlace(evt *pb.AdEvent) {
	evt.CampaignId = evt.CampaignId[:0]
	evt.EventType = evt.EventType[:0]
	if evt.Metadata != nil {
		evt.Metadata.ClickId = evt.Metadata.ClickId[:0]
		evt.Metadata.UserId = evt.Metadata.UserId[:0]
		evt.Metadata.DeviceType = evt.Metadata.DeviceType[:0]
		evt.Metadata.Os = evt.Metadata.Os[:0]
		for i := range evt.Metadata.ExtraKeys {
			evt.Metadata.ExtraKeys[i] = evt.Metadata.ExtraKeys[i][:0]
		}
		evt.Metadata.ExtraKeys = evt.Metadata.ExtraKeys[:0]
		for i := range evt.Metadata.ExtraValues {
			evt.Metadata.ExtraValues[i] = evt.Metadata.ExtraValues[i][:0]
		}
		evt.Metadata.ExtraValues = evt.Metadata.ExtraValues[:0]
		evt.Metadata.ExtraBytes = evt.Metadata.ExtraBytes[:0]
	}
}

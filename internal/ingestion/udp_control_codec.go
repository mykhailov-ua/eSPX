package ingestion

import (
	"encoding/binary"
	"hash/fnv"
)

const (
	udpMagic            uint32 = 0x45535058 // "ESPX"
	udpProtocolVersion  uint8  = 1
	udpProtocolVersion2 uint8  = 2
	UDPHeaderSize              = 48
	UDPMaxControlShards        = 16
	udpCanaryFloorRPS          = 100
	udpCanaryFloorPct          = 5

	UDPMsgQuotaEpoch       uint8 = 1
	UDPMsgConfigSnapshot   uint8 = 2
	UDPMsgConfigRequest    uint8 = 3
	UDPMsgMigrationBarrier uint8 = 4

	UDPFlagSnapshot uint16 = 1 << 0
)

// UDPControlLimits carries per-shard ingress RPS limits in a fixed array (zero heap on hot decode).
type UDPControlLimits struct {
	NumShards uint8
	Limits    [UDPMaxControlShards]uint64
	MaxRPD    uint64 // per-region daily ingress cap (protocol v2 extension)
}

// UDPHeader is the fixed 48-byte control datagram prefix (little-endian).
type UDPHeader struct {
	Magic          uint32
	Version        uint8
	MsgType        uint8
	Flags          uint16
	CoarseTimeNs   int64
	EpochID        int64
	ConfigHash     [16]byte
	SlotMapVersion int32
	NumShards      uint8
	PayloadLen     uint16
}

// UDPConfigRequestPayload is tracker → management CONFIG_REQUEST body.
type UDPConfigRequestPayload struct {
	TrackerID uint32
	LastEpoch int64
	Hash      [16]byte
}

// UDPMigrationBarrierPayload is management → tracker MIGRATION_BARRIER body.
type UDPMigrationBarrierPayload struct {
	MigrationGen int64
	Draining     [128]byte // 1024-bit slot bitmap
}

func udpShardPayloadLen(numShards uint8) int {
	if numShards == 0 || int(numShards) > UDPMaxControlShards {
		return 0
	}
	return int(numShards) * 8
}

// DecodeUDPHeader parses the fixed datagram header.
func DecodeUDPHeader(src []byte, hdr *UDPHeader) bool {
	return udpDecodeHeader(src, hdr)
}

// DecodeUDPConfigRequest parses CONFIG_REQUEST payload.
func DecodeUDPConfigRequest(payload []byte, req *UDPConfigRequestPayload) bool {
	return udpDecodeConfigRequest(payload, req)
}

func udpEncodeHeader(dst []byte, hdr *UDPHeader) int {
	if len(dst) < UDPHeaderSize {
		return 0
	}
	binary.LittleEndian.PutUint32(dst[0:4], hdr.Magic)
	dst[4] = hdr.Version
	dst[5] = hdr.MsgType
	binary.LittleEndian.PutUint16(dst[6:8], hdr.Flags)
	binary.LittleEndian.PutUint64(dst[8:16], uint64(hdr.CoarseTimeNs))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(hdr.EpochID))
	copy(dst[24:40], hdr.ConfigHash[:])
	binary.LittleEndian.PutUint32(dst[40:44], uint32(hdr.SlotMapVersion))
	dst[44] = hdr.NumShards
	dst[45] = 0
	binary.LittleEndian.PutUint16(dst[46:48], hdr.PayloadLen)
	return UDPHeaderSize
}

func udpDecodeHeader(src []byte, hdr *UDPHeader) bool {
	if len(src) < UDPHeaderSize {
		return false
	}
	hdr.Magic = binary.LittleEndian.Uint32(src[0:4])
	if hdr.Magic != udpMagic {
		return false
	}
	if src[4] != udpProtocolVersion && src[4] != udpProtocolVersion2 {
		return false
	}
	hdr.Version = src[4]
	hdr.MsgType = src[5]
	hdr.Flags = binary.LittleEndian.Uint16(src[6:8])
	hdr.CoarseTimeNs = int64(binary.LittleEndian.Uint64(src[8:16]))
	hdr.EpochID = int64(binary.LittleEndian.Uint64(src[16:24]))
	copy(hdr.ConfigHash[:], src[24:40])
	hdr.SlotMapVersion = int32(binary.LittleEndian.Uint32(src[40:44]))
	hdr.NumShards = src[44]
	hdr.PayloadLen = binary.LittleEndian.Uint16(src[46:48])
	return true
}

func udpDecodeShardLimits(payload []byte, numShards uint8, out *UDPControlLimits) bool {
	if out == nil {
		return false
	}
	need := udpShardPayloadLen(numShards)
	if len(payload) < need {
		return false
	}
	out.NumShards = numShards
	for i := uint8(0); i < numShards; i++ {
		out.Limits[i] = binary.LittleEndian.Uint64(payload[i*8 : i*8+8])
	}
	out.MaxRPD = 0
	if len(payload) >= need+8 {
		out.MaxRPD = binary.LittleEndian.Uint64(payload[need : need+8])
	}
	return true
}

func udpEncodeShardLimits(dst []byte, limits *UDPControlLimits) int {
	if limits == nil || limits.NumShards == 0 {
		return 0
	}
	need := udpShardPayloadLen(limits.NumShards)
	extra := 0
	if limits.MaxRPD > 0 {
		extra = 8
	}
	if len(dst) < need+extra {
		return 0
	}
	for i := uint8(0); i < limits.NumShards; i++ {
		binary.LittleEndian.PutUint64(dst[i*8:i*8+8], limits.Limits[i])
	}
	if extra > 0 {
		binary.LittleEndian.PutUint64(dst[need:need+8], limits.MaxRPD)
	}
	return need + extra
}

func udpEncodeConfigRequest(dst []byte, req *UDPConfigRequestPayload) int {
	if len(dst) < 28 || req == nil {
		return 0
	}
	binary.LittleEndian.PutUint32(dst[0:4], req.TrackerID)
	binary.LittleEndian.PutUint64(dst[4:12], uint64(req.LastEpoch))
	copy(dst[12:28], req.Hash[:])
	return 28
}

func udpDecodeConfigRequest(payload []byte, req *UDPConfigRequestPayload) bool {
	if len(payload) < 28 || req == nil {
		return false
	}
	req.TrackerID = binary.LittleEndian.Uint32(payload[0:4])
	req.LastEpoch = int64(binary.LittleEndian.Uint64(payload[4:12]))
	copy(req.Hash[:], payload[12:28])
	return true
}

// ComputeUDPConfigHash derives a 128-bit FNV digest for epoch idempotency.
func ComputeUDPConfigHash(epoch int64, slotVersion int32, limits *UDPControlLimits) [16]byte {
	var out [16]byte
	if limits == nil {
		return out
	}
	h := fnv.New128a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(epoch))
	_, _ = h.Write(buf[:])
	binary.LittleEndian.PutUint32(buf[:4], uint32(slotVersion))
	_, _ = h.Write(buf[:4])
	for i := uint8(0); i < limits.NumShards; i++ {
		binary.LittleEndian.PutUint64(buf[:], limits.Limits[i])
		_, _ = h.Write(buf[:])
	}
	if limits.MaxRPD > 0 {
		binary.LittleEndian.PutUint64(buf[:], limits.MaxRPD)
		_, _ = h.Write(buf[:])
	}
	sum := h.Sum(nil)
	copy(out[:], sum[:16])
	return out
}

func udpLimitsTightening(prev, next *UDPControlLimits) bool {
	if next == nil {
		return true
	}
	if prev == nil {
		return false
	}
	n := prev.NumShards
	if next.NumShards < n {
		n = next.NumShards
	}
	for i := uint8(0); i < n; i++ {
		if next.Limits[i] > prev.Limits[i] {
			return false
		}
	}
	return true
}

func udpApplyCanaryFloor(limits *UDPControlLimits) {
	if limits == nil || limits.NumShards == 0 {
		return
	}
	for i := uint8(0); i < limits.NumShards; i++ {
		floor := uint64(udpCanaryFloorRPS)
		if limits.Limits[i] > 0 {
			pct := limits.Limits[i] * uint64(udpCanaryFloorPct) / 100
			if pct > floor {
				floor = pct
			}
		}
		limits.Limits[i] = floor
	}
}

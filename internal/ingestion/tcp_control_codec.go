package ingestion

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	tcpControlMagic      uint32 = 0x45535058 // "ESPX"
	tcpControlVersion    uint8  = 1
	tcpControlHMACSize          = 32
	tcpControlPrefixSize        = 32 // bytes signed before HMAC field

	TCPMsgSnapshot        uint8 = 1
	TCPMsgSnapshotRequest uint8 = 2
	TCPMsgAck             uint8 = 3
)

const TCPControlHeaderSize = 64

// TCPControlHeader is the fixed 64-byte routing cutover frame prefix (M2 GAP-SHARD-05).
type TCPControlHeader struct {
	Magic          uint32
	Version        uint8
	MsgType        uint8
	Flags          uint16
	RoutingEpoch   int64
	SlotMapVersion int32
	TrackerID      uint32
	PayloadLen     uint32
	NumShards      uint8
	HMAC           [tcpControlHMACSize]byte
}

// TCPAckPayload is tracker → management ACK body.
type TCPAckPayload struct {
	TrackerID      uint32
	AppliedEpoch   int64
	AppliedSlotVer int32
}

var (
	ErrTCPControlCorrupt = errors.New("tcp control: corrupt frame")
	ErrTCPControlHMAC    = errors.New("tcp control: hmac mismatch")
)

// EncodeTCPControlFrame writes a signed control frame into dst.
func EncodeTCPControlFrame(dst []byte, secret []byte, hdr *TCPControlHeader, payload []byte) (int, error) {
	if hdr == nil {
		return 0, ErrTCPControlCorrupt
	}
	pl := len(payload)
	if len(dst) < TCPControlHeaderSize+pl {
		return 0, ErrTCPControlCorrupt
	}
	hdr.Magic = tcpControlMagic
	hdr.Version = tcpControlVersion
	hdr.PayloadLen = uint32(pl)
	tcpEncodePrefix(dst, hdr)
	if pl > 0 {
		copy(dst[TCPControlHeaderSize:], payload)
	}
	sum := computeTCPControlHMAC(secret, dst[:tcpControlPrefixSize], payload)
	copy(dst[32:64], sum[:])
	hdr.HMAC = sum
	return TCPControlHeaderSize + pl, nil
}

// DecodeTCPControlFrame parses and verifies a signed control frame.
func DecodeTCPControlFrame(src []byte, secret []byte, hdr *TCPControlHeader) ([]byte, error) {
	if hdr == nil || len(src) < TCPControlHeaderSize {
		return nil, ErrTCPControlCorrupt
	}
	if !tcpDecodePrefix(src, hdr) {
		return nil, ErrTCPControlCorrupt
	}
	if int(hdr.PayloadLen) > len(src)-TCPControlHeaderSize {
		return nil, ErrTCPControlCorrupt
	}
	payload := src[TCPControlHeaderSize : TCPControlHeaderSize+int(hdr.PayloadLen)]
	copy(hdr.HMAC[:], src[32:64])
	want := computeTCPControlHMAC(secret, src[:tcpControlPrefixSize], payload)
	if len(secret) > 0 && !hmac.Equal(want[:], hdr.HMAC[:]) {
		return nil, ErrTCPControlHMAC
	}
	out := make([]byte, len(payload))
	copy(out, payload)
	return out, nil
}

// EncodeTCPAckPayload serializes tracker ACK body.
func EncodeTCPAckPayload(dst []byte, ack *TCPAckPayload) int {
	if ack == nil || len(dst) < 16 {
		return 0
	}
	binary.LittleEndian.PutUint32(dst[0:4], ack.TrackerID)
	binary.LittleEndian.PutUint64(dst[4:12], uint64(ack.AppliedEpoch))
	binary.LittleEndian.PutUint32(dst[12:16], uint32(ack.AppliedSlotVer))
	return 16
}

// DecodeTCPAckPayload parses tracker ACK body.
func DecodeTCPAckPayload(payload []byte, ack *TCPAckPayload) bool {
	if ack == nil || len(payload) < 16 {
		return false
	}
	ack.TrackerID = binary.LittleEndian.Uint32(payload[0:4])
	ack.AppliedEpoch = int64(binary.LittleEndian.Uint64(payload[4:12]))
	ack.AppliedSlotVer = int32(binary.LittleEndian.Uint32(payload[12:16]))
	return true
}

// EncodeTCPLimitsPayload serializes shard limits for TCP snapshot bodies.
func EncodeTCPLimitsPayload(dst []byte, limits *UDPControlLimits) int {
	return udpEncodeShardLimits(dst, limits)
}

func computeTCPControlHMAC(secret, headerPrefix []byte, payload []byte) [tcpControlHMACSize]byte {
	var out [tcpControlHMACSize]byte
	if len(secret) == 0 {
		return out
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(headerPrefix)
	if len(payload) > 0 {
		_, _ = mac.Write(payload)
	}
	sum := mac.Sum(nil)
	copy(out[:], sum[:tcpControlHMACSize])
	return out
}

func tcpEncodePrefix(dst []byte, hdr *TCPControlHeader) {
	binary.LittleEndian.PutUint32(dst[0:4], hdr.Magic)
	dst[4] = hdr.Version
	dst[5] = hdr.MsgType
	binary.LittleEndian.PutUint16(dst[6:8], hdr.Flags)
	binary.LittleEndian.PutUint64(dst[8:16], uint64(hdr.RoutingEpoch))
	binary.LittleEndian.PutUint32(dst[16:20], uint32(hdr.SlotMapVersion))
	binary.LittleEndian.PutUint32(dst[20:24], hdr.TrackerID)
	binary.LittleEndian.PutUint32(dst[24:28], hdr.PayloadLen)
	dst[28] = hdr.NumShards
	dst[29] = 0
	dst[30] = 0
	dst[31] = 0
}

func tcpDecodePrefix(src []byte, hdr *TCPControlHeader) bool {
	if len(src) < tcpControlPrefixSize {
		return false
	}
	hdr.Magic = binary.LittleEndian.Uint32(src[0:4])
	if hdr.Magic != tcpControlMagic {
		return false
	}
	hdr.Version = src[4]
	if hdr.Version != tcpControlVersion {
		return false
	}
	hdr.MsgType = src[5]
	hdr.Flags = binary.LittleEndian.Uint16(src[6:8])
	hdr.RoutingEpoch = int64(binary.LittleEndian.Uint64(src[8:16]))
	hdr.SlotMapVersion = int32(binary.LittleEndian.Uint32(src[16:20]))
	hdr.TrackerID = binary.LittleEndian.Uint32(src[20:24])
	hdr.PayloadLen = binary.LittleEndian.Uint32(src[24:28])
	hdr.NumShards = src[28]
	return true
}

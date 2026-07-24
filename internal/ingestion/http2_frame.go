package ingestion

import "unsafe"

// http2_frame.go — HTTP/2 frame header FSM (RFC 9113, M5-C1).

const (
	h2FrameHeaderSize  = 9
	h2ClientPrefaceLen = 24

	h2FlagEndStream  byte = 0x1
	h2FlagEndHeaders byte = 0x4
)

const (
	h2FrameData         byte = 0x0
	h2FrameHeaders      byte = 0x1
	h2FramePriority     byte = 0x2
	h2FrameRSTStream    byte = 0x3
	h2FrameSettings     byte = 0x4
	h2FramePushPromise  byte = 0x5
	h2FramePing         byte = 0x6
	h2FrameGoAway       byte = 0x7
	h2FrameWindowUpdate byte = 0x8
	h2FrameContinuation byte = 0x9
)

var h2ClientPreface = [24]byte{
	'P', 'R', 'I', ' ', '*', ' ', 'H', 'T', 'T', 'P', '/', '2', '.', '0', '\r', '\n',
	'\r', '\n', 'S', 'M', '\r', '\n', '\r', '\n',
}

type h2Frame struct {
	Length   uint32
	Type     byte
	Flags    byte
	StreamID uint32
	Payload  []byte
}

func isH2ClientPreface(buf []byte) bool {
	if len(buf) < h2ClientPrefaceLen {
		return false
	}
	return *(*[24]byte)(unsafe.Pointer(&buf[0])) == h2ClientPreface
}

// decodeH2FrameHeader parses the 9-byte frame header (0 allocs/op).
func decodeH2FrameHeader(buf []byte) (h2Frame, int, error) {
	var fr h2Frame
	if len(buf) < h2FrameHeaderSize {
		return fr, 0, errIncompleteRequest
	}
	_ = buf[8]
	fr.Length = uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])
	fr.Type = buf[3]
	fr.Flags = buf[4]
	fr.StreamID = uint32(buf[5]&0x7f)<<24 | uint32(buf[6])<<16 | uint32(buf[7])<<8 | uint32(buf[8])
	total := h2FrameHeaderSize + int(fr.Length)
	if len(buf) < total {
		return fr, 0, errIncompleteRequest
	}
	if fr.Length > 0 {
		fr.Payload = buf[h2FrameHeaderSize:total]
	}
	return fr, total, nil
}

// encodeH2FrameHeader writes a 9-byte frame header into dst (must have len >= 9).
func encodeH2FrameHeader(dst []byte, length uint32, typ, flags byte, streamID uint32) int {
	dst[0] = byte(length >> 16)
	dst[1] = byte(length >> 8)
	dst[2] = byte(length)
	dst[3] = typ
	dst[4] = flags
	dst[5] = byte(streamID >> 24)
	dst[6] = byte(streamID >> 16)
	dst[7] = byte(streamID >> 8)
	dst[8] = byte(streamID)
	return h2FrameHeaderSize
}

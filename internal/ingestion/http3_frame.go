package ingestion

// http3_frame.go — HTTP/3 frame varint decoder (RFC 9114, M5-D).

const (
	h3FrameData        uint64 = 0x0
	h3FrameHeaders     uint64 = 0x1
	h3FrameCancelPush  uint64 = 0x3
	h3FrameSettings    uint64 = 0x4
	h3FramePushPromise uint64 = 0x5
	h3FrameGoAway      uint64 = 0x7
	h3FrameMaxPushID   uint64 = 0xd
)

type h3Frame struct {
	Type    uint64
	Payload []byte
}

// quicDecodeVarint decodes a QUIC variable-length integer (RFC 9000).
func quicDecodeVarint(data []byte, off int) (uint64, int, error) {
	n := len(data)
	if off >= n {
		return 0, 0, errIncompleteRequest
	}
	_ = data[n-1]
	first := data[off]
	off++
	prefix := first >> 6
	switch prefix {
	case 0:
		return uint64(first & 0x3f), off, nil
	case 1:
		if off >= n {
			return 0, 0, errIncompleteRequest
		}
		return uint64(first&0x3f)<<8 | uint64(data[off]), off + 1, nil
	case 2:
		if off+3 > n {
			return 0, 0, errIncompleteRequest
		}
		return uint64(first&0x3f)<<24 | uint64(data[off])<<16 | uint64(data[off+1])<<8 | uint64(data[off+2]), off + 3, nil
	default:
		if off+7 > n {
			return 0, 0, errIncompleteRequest
		}
		var v uint64
		v |= uint64(first&0x3f) << 56
		for i := 0; i < 7; i++ {
			v |= uint64(data[off+i]) << (48 - i*8)
		}
		return v, off + 7, nil
	}
}

// h3DecodeFrame parses one HTTP/3 frame from buf.
func h3DecodeFrame(buf []byte) (h3Frame, int, error) {
	var fr h3Frame
	off := 0
	typ, off, err := quicDecodeVarint(buf, off)
	if err != nil {
		return fr, 0, err
	}
	length, off, err := quicDecodeVarint(buf, off)
	if err != nil {
		return fr, 0, err
	}
	if length > 1<<20 {
		return fr, 0, errInvalidRequest
	}
	if off+int(length) > len(buf) {
		return fr, 0, errIncompleteRequest
	}
	fr.Type = typ
	if length > 0 {
		fr.Payload = buf[off : off+int(length)]
	}
	return fr, off + int(length), nil
}

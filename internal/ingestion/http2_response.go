package ingestion

// http2_response.go — prebuilt HTTP/2 HEADERS + DATA response encoder (M5-C4).

var (
	h2ServerSettings = []byte{
		0x00, 0x00, 0x0c, h2FrameSettings, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x03, 0x00, 0x00, 0x00, 0x80,
		0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
	}
	h2SettingsACK = []byte{
		0x00, 0x00, 0x00, h2FrameSettings, 0x01, 0x00, 0x00, 0x00, 0x00,
	}
	// h2ConnBootstrap is the fixed server SETTINGS + ACK burst on first client preface (0 allocs/op).
	h2ConnBootstrap = append(append([]byte(nil), h2ServerSettings...), h2SettingsACK...)
)

// h2EncodeStatusResponse encodes HEADERS + optional DATA for a status/body pair.
func h2EncodeStatusResponse(dst []byte, streamID uint32, status int, contentType, body []byte) int {
	blockOff := h2FrameHeaderSize
	block := dst[blockOff : blockOff+256]
	boff := 0
	block[boff] = 0x00
	boff++
	boff = h2EncodeInt(block, boff, 8, 4, 0x0f)
	boff = h2EncodeString(block, boff, []byte(h2StatusString(status)))
	if len(contentType) > 0 {
		block[boff] = 0x00
		boff++
		boff = h2EncodeInt(block, boff, 31, 4, 0x0f)
		if len(contentType) != 16 || !bytesEqual(contentType, "application/json") {
			boff = h2EncodeString(block, boff, contentType)
		}
	}
	blockLen := boff
	hdrFlags := h2FlagEndHeaders
	if len(body) == 0 {
		hdrFlags |= h2FlagEndStream
	}
	encodeH2FrameHeader(dst, uint32(blockLen), h2FrameHeaders, hdrFlags, streamID)
	copy(dst[h2FrameHeaderSize:], block[:blockLen])
	n := h2FrameHeaderSize + blockLen
	if len(body) > 0 {
		n = h2EncodeDataFrame(dst, n, streamID, body, true)
	}
	return n
}

func h2StatusString(code int) string {
	switch code {
	case 200:
		return "200"
	case 202:
		return "202"
	case 204:
		return "204"
	case 400:
		return "400"
	case 404:
		return "404"
	case 413:
		return "413"
	case 429:
		return "429"
	case 500:
		return "500"
	case 503:
		return "503"
	default:
		return "500"
	}
}

func h2EncodeDataFrame(dst []byte, off int, streamID uint32, payload []byte, endStream bool) int {
	flags := byte(0)
	if endStream {
		flags = h2FlagEndStream
	}
	encodeH2FrameHeader(dst[off:], uint32(len(payload)), h2FrameData, flags, streamID)
	off += h2FrameHeaderSize
	off += copy(dst[off:], payload)
	return off
}

// h2WrapH1Response converts a prebuilt H1 response into H2 HEADERS+DATA frames.
func h2WrapH1Response(dst []byte, streamID uint32, h1 []byte) (int, error) {
	status, body, contentType, ok := parseH1ResponseForH2(h1)
	if !ok {
		return 0, errInvalidRequest
	}
	return h2EncodeStatusResponse(dst, streamID, status, contentType, body), nil
}

func parseH1ResponseForH2(h1 []byte) (status int, body, contentType []byte, ok bool) {
	if len(h1) < 12 || !bytesEqual(h1[:5], "HTTP/") {
		return 0, nil, nil, false
	}
	code := 0
	digits := 0
	for i := 5; i < len(h1) && i < 24; i++ {
		if h1[i] == ' ' {
			if digits > 0 {
				break
			}
			continue
		}
		if h1[i] >= '0' && h1[i] <= '9' {
			code = code*10 + int(h1[i]-'0')
			digits++
			continue
		}
		if digits > 0 {
			break
		}
	}
	if digits == 0 {
		return 0, nil, nil, false
	}
	hdrEnd := -1
	for i := 0; i+3 < len(h1); i++ {
		if h1[i] == '\r' && h1[i+1] == '\n' && h1[i+2] == '\r' && h1[i+3] == '\n' {
			hdrEnd = i + 4
			break
		}
	}
	if hdrEnd < 0 {
		return 0, nil, nil, false
	}
	body = h1[hdrEnd:]
	line := 0
	for lineStart := 0; lineStart < hdrEnd-1; {
		lineEnd := lineStart
		for lineEnd+1 < hdrEnd && !(h1[lineEnd] == '\r' && h1[lineEnd+1] == '\n') {
			lineEnd++
		}
		if line > 0 {
			colon := -1
			for j := lineStart; j < lineEnd; j++ {
				if h1[j] == ':' {
					colon = j
					break
				}
			}
			if colon > 0 {
				key := trimHTTPKey(h1[lineStart:colon])
				if len(key) == 12 && foldKeyU64(key, 0) == 0x2d746e65746e6f63 && foldKeyU32(key, 8) == 0x65707974 {
					contentType = trimHTTPVal(h1[colon+1 : lineEnd])
				}
			}
		}
		line++
		lineStart = lineEnd + 2
	}
	return code, body, contentType, true
}

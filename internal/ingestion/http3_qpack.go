package ingestion

// http3_qpack.go — subset QPACK decoder for HTTP/3 (RFC 9204, M5-D2).
// Static table matches HPACK; field line encoding reuses h2DecodeHeadersBlock.

// h3DecodeQPACKBlock decodes a QPACK HEADERS frame payload into parsedHTTPRequest.
func h3DecodeQPACKBlock(block []byte, req *parsedHTTPRequest) error {
	return h2DecodeHeadersBlock(block, req)
}

// h3ParseRequestFrames extracts one POST /track from a bidirectional H3 stream buffer.
func h3ParseRequestFrames(buf []byte, maxBody int64) (consumed int, req parsedHTTPRequest, err error) {
	off := 0
	gotHeaders := false
	for off < len(buf) {
		fr, n, ferr := h3DecodeFrame(buf[off:])
		if ferr != nil {
			return off, req, ferr
		}
		off += n
		switch fr.Type {
		case h3FrameHeaders:
			if err := h3DecodeQPACKBlock(fr.Payload, &req); err != nil {
				return off, req, err
			}
			gotHeaders = true
		case h3FrameData:
			if !gotHeaders {
				return off, req, errInvalidRequest
			}
			if int64(len(fr.Payload)) > maxBody {
				return off, req, errPayloadTooLarge
			}
			req.Body = fr.Payload
			req.ContentLength = len(fr.Payload)
			req.HasContentLength = true
			return off, req, nil
		default:
			continue
		}
	}
	if gotHeaders && len(req.Body) == 0 && !req.HasContentLength {
		return off, req, nil
	}
	return off, req, errIncompleteRequest
}

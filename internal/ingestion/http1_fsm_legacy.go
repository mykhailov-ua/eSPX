package ingestion

import "bytes"

// parseHTTPLegacy is the pre-M5-B bytes.Index parser kept for benchmark comparison only.
func parseHTTPLegacy(data []byte, maxBody int64) (int, parsedHTTPRequest, error) {
	var req parsedHTTPRequest

	lineEnd := bytes.Index(data, []byte("\r\n"))
	if lineEnd < 0 {
		return 0, req, errIncompleteRequest
	}
	reqLine := data[:lineEnd]

	space1 := bytes.IndexByte(reqLine, ' ')
	if space1 < 0 {
		return 0, req, errInvalidRequest
	}
	req.Method = reqLine[:space1]

	rest := reqLine[space1+1:]
	space2 := bytes.IndexByte(rest, ' ')
	if space2 < 0 {
		return 0, req, errInvalidRequest
	}
	req.Path = rest[:space2]

	idx := lineEnd + 2
	for {
		if idx >= len(data) {
			return 0, req, errIncompleteRequest
		}
		if idx+2 <= len(data) && data[idx] == '\r' && data[idx+1] == '\n' {
			idx += 2
			break
		}

		lineEnd = bytes.Index(data[idx:], []byte("\r\n"))
		if lineEnd < 0 {
			return 0, req, errIncompleteRequest
		}
		headerLine := data[idx : idx+lineEnd]
		idx += lineEnd + 2

		colonIdx := bytes.IndexByte(headerLine, ':')
		if colonIdx < 0 {
			continue
		}

		key := trimSpaceBytes(headerLine[:colonIdx])
		val := trimSpaceBytes(headerLine[colonIdx+1:])

		assignHTTPHeaderLegacy(&req, key, val)
	}

	if req.HasContentLength && int64(req.ContentLength) > maxBody {
		return 0, req, errPayloadTooLarge
	}

	totalLen := idx + req.ContentLength
	if len(data) < totalLen {
		return 0, req, errIncompleteRequest
	}
	req.Body = data[idx : idx+req.ContentLength]
	return totalLen, req, nil
}

func assignHTTPHeaderLegacy(req *parsedHTTPRequest, key, val []byte) {
	if equalFoldBytes(key, []byte("content-length")) {
		req.ContentLength = parseDecimal(val)
		req.HasContentLength = true
	} else if equalFoldBytes(key, []byte("content-type")) {
		req.ContentType = val
	} else if equalFoldBytes(key, []byte("x-forwarded-for")) {
		req.ClientIP = val
	} else if equalFoldBytes(key, []byte("x-real-ip")) {
		if len(req.ClientIP) == 0 {
			req.ClientIP = val
		}
	} else if equalFoldBytes(key, []byte("user-agent")) {
		req.UserAgent = val
	} else if equalFoldBytes(key, []byte("accept")) {
		req.Accept = val
	} else if equalFoldBytes(key, []byte("x-tls-hash")) {
		req.TLSHash = val
	} else if equalFoldBytes(key, []byte("sec-ch-ua")) {
		req.SecCHUA = val
	} else if equalFoldBytes(key, []byte("accept-language")) {
		req.AcceptLang = val
	}
}

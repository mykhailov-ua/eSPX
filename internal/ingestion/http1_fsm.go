package ingestion

import "unsafe"

// http1_fsm.go — table-driven HTTP/1.1 request-line + header FSM for gnet /track ingress (M5-B).

const (
	http1MaxMethodLen     = 16
	http1MaxPathLen       = 2048
	http1MaxHeaderNameLen = 256
	http1MaxHeaderValLen  = 1024
)

var (
	httpFold [256]byte

	// POST /track HTTP/1.1\r\n — nginx keepalive fast path (22 bytes).
	trackReqLine = [22]byte{
		'P', 'O', 'S', 'T', ' ', '/', 't', 'r', 'a', 'c', 'k', ' ',
		'H', 'T', 'T', 'P', '/', '1', '.', '1', '\r', '\n',
	}
)

func init() {
	for i := 0; i < 256; i++ {
		httpFold[i] = byte(i)
	}
	for i := 'A'; i <= 'Z'; i++ {
		httpFold[i] = byte(i + ('a' - 'A'))
	}
}

const (
	http1flChunkedTE uint8 = 1 << iota
	http1flHasTE
	http1flInvalidTE
	http1flCLSet
)

// parseHTTP1 extracts one HTTP/1.1 request from data without copying the body (0 allocs/op on common path).
func parseHTTP1(data []byte, maxBody int64, scratch ...[]byte) (int, parsedHTTPRequest, error) {
	var chunkScratch []byte
	if len(scratch) > 0 {
		chunkScratch = scratch[0]
	}
	var req parsedHTTPRequest
	var hFlags uint8
	var clValue int
	n := len(data)
	if n == 0 {
		return 0, req, errIncompleteRequest
	}
	_ = data[n-1] // BCE window for all indexed reads below.

	i := 0

	// Fast path: exact POST /track HTTP/1.1\r\n (no query string).
	if n >= 22 && *(*[22]byte)(unsafe.Pointer(&data[0])) == trackReqLine {
		req.Method = data[:4]
		req.Path = data[5:11]
		i = 22
	} else {
		sp1, sp2 := -1, -1
		for i < n {
			b := data[i]
			if b == '\r' {
				if i+1 >= n {
					return 0, req, errIncompleteRequest
				}
				if data[i+1] != '\n' {
					return 0, req, errInvalidRequest
				}
				if sp1 < 0 || sp2 < 0 {
					return 0, req, errInvalidRequest
				}
				req.Method = data[:sp1]
				req.Path = data[sp1+1 : sp2]
				if len(req.Method) > http1MaxMethodLen || len(req.Path) > http1MaxPathLen {
					return 0, req, errInvalidRequest
				}
				if !httpTokenValid(req.Method) || !httpPathValid(req.Path) || !http1VersionValid(data[sp2+1:i]) {
					return 0, req, errInvalidRequest
				}
				if !http1IngressValid(req.Method, req.Path) {
					return 0, req, errInvalidRequest
				}
				i += 2
				goto headers
			}
			if b == 0 || (b < 0x20 && b != '\t') {
				return 0, req, errInvalidRequest
			}
			if b == ' ' {
				if sp1 < 0 {
					sp1 = i
				} else if sp2 < 0 {
					sp2 = i
				}
				i++
				continue
			}
			i++
		}
		return 0, req, errIncompleteRequest
	}

headers:
	for {
		if i >= n {
			return 0, req, errIncompleteRequest
		}
		if data[i] == '\r' {
			if i+1 >= n {
				return 0, req, errIncompleteRequest
			}
			if data[i+1] != '\n' {
				return 0, req, errInvalidRequest
			}
			i += 2
			break
		}

		lineStart := i
		colon := -1
		for i < n {
			b := data[i]
			if b == 0 || (b < 0x20 && b != '\t') {
				return 0, req, errInvalidRequest
			}
			if b == ':' {
				colon = i
				i++
				break
			}
			if b == '\r' {
				return 0, req, errInvalidRequest
			}
			i++
		}
		if colon < 0 {
			return 0, req, errIncompleteRequest
		}

		for i < n && data[i] != '\r' {
			if data[i] == 0 {
				return 0, req, errInvalidRequest
			}
			i++
		}
		if i+1 >= n {
			return 0, req, errIncompleteRequest
		}
		if data[i+1] != '\n' {
			return 0, req, errInvalidRequest
		}

		key := trimHTTPKey(data[lineStart:colon])
		val := trimHTTPVal(data[colon+1 : i])
		if len(key) == 0 || len(key) > http1MaxHeaderNameLen || !httpTokenValid(key) {
			return 0, req, errInvalidRequest
		}
		if len(val) > http1MaxHeaderValLen || !httpHeaderValValid(val) {
			return 0, req, errInvalidRequest
		}
		if err := http1AssignHeader(&req, key, val, &hFlags, &clValue); err != nil {
			return 0, req, err
		}
		i += 2
	}

	if hFlags&http1flInvalidTE != 0 {
		return 0, req, errInvalidRequest
	}

	if hFlags&http1flChunkedTE != 0 {
		if hFlags&http1flCLSet != 0 {
			return 0, req, errInvalidRequest
		}
		consumed, body, cl, scratchOut, err := parseHTTP1ChunkedBody(data, i, maxBody, chunkScratch)
		if err != nil {
			return 0, req, err
		}
		req.Body = body
		req.ContentLength = cl
		req.HasContentLength = true
		_ = scratchOut
		return consumed, req, nil
	}

	if hFlags&http1flHasTE != 0 {
		return 0, req, errInvalidRequest
	}

	if req.HasContentLength && int64(req.ContentLength) > maxBody {
		return 0, req, errPayloadTooLarge
	}
	total := i + req.ContentLength
	if n < total {
		return 0, req, errIncompleteRequest
	}
	if req.ContentLength > 0 {
		req.Body = data[i : i+req.ContentLength]
	}
	return total, req, nil
}

func httpTokenValid(b []byte) bool {
	for _, c := range b {
		if c < 0x21 || c > 0x7E {
			return false
		}
	}
	return len(b) > 0
}

func httpPathValid(b []byte) bool {
	for _, c := range b {
		if c == 0 || c < 0x20 || c > 0x7E {
			return false
		}
	}
	return len(b) > 0
}

// http1IngressValid restricts tracker ingress to POST /track and probe GET paths.
func http1IngressValid(method, path []byte) bool {
	if len(method) == 4 && method[0] == 'P' && method[1] == 'O' && method[2] == 'S' && method[3] == 'T' {
		return httpPathHasPrefix(path, "/track") || httpPathHasPrefix(path, "/openrtb/bid")
	}
	if len(method) == 3 && method[0] == 'G' && method[1] == 'E' && method[2] == 'T' {
		return bytesEqual(path, "/health") ||
			bytesEqual(path, "/healthz") ||
			bytesEqual(path, "/readyz") ||
			bytesEqual(path, "/metrics")
	}
	return false
}

func httpPathHasPrefix(path []byte, prefix string) bool {
	p := []byte(prefix)
	if len(path) < len(p) {
		return false
	}
	if !bytesEqual(path[:len(p)], prefix) {
		return false
	}
	if len(path) == len(p) {
		return true
	}
	switch path[len(p)] {
	case '?', '/':
		return true
	default:
		return false
	}
}

func bytesEqual(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}

func teValueHasChunked(val []byte) bool {
	i := 0
	for i < len(val) {
		for i < len(val) && (val[i] == ' ' || val[i] == '\t' || val[i] == ',') {
			i++
		}
		if i >= len(val) {
			break
		}
		start := i
		for i < len(val) && val[i] != ',' {
			i++
		}
		token := val[start:i]
		if len(token) == 7 &&
			foldKeyU32(token, 0) == 0x6e756863 &&
			httpFold[token[4]] == 'k' &&
			httpFold[token[5]] == 'e' &&
			httpFold[token[6]] == 'd' {
			return true
		}
	}
	return false
}

func http1VersionValid(b []byte) bool {
	return len(b) == 8 &&
		foldKeyU32(b, 0) == 0x70747468 &&
		foldKeyU32(b, 4) == 0x312e312f
}

func httpHeaderValValid(b []byte) bool {
	for _, c := range b {
		if c == 0 || c == '\r' || c == '\n' {
			return false
		}
		if c < 0x20 && c != '\t' {
			return false
		}
		if c == 0x7F {
			return false
		}
	}
	return true
}

// parseContentLengthStrict parses Content-Length digits only (rejects empty, alpha, overflow).
func parseContentLengthStrict(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	val := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		if val > (1<<31-1)/10 {
			return 0, false
		}
		next := val*10 + int(c-'0')
		if next < val {
			return 0, false
		}
		val = next
	}
	return val, true
}

func trimHTTPKey(b []byte) []byte {
	end := len(b)
	for end > 0 && (b[end-1] == ' ' || b[end-1] == '\t') {
		end--
	}
	return b[:end]
}

func trimHTTPVal(b []byte) []byte {
	start := 0
	for start < len(b) && (b[start] == ' ' || b[start] == '\t') {
		start++
	}
	end := len(b)
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t') {
		end--
	}
	return b[start:end]
}

func foldKeyU32(key []byte, off int) uint32 {
	return uint32(httpFold[key[off]]) |
		uint32(httpFold[key[off+1]])<<8 |
		uint32(httpFold[key[off+2]])<<16 |
		uint32(httpFold[key[off+3]])<<24
}

func foldKeyU64(key []byte, off int) uint64 {
	return uint64(foldKeyU32(key, off)) |
		uint64(foldKeyU32(key, off+4))<<32
}

// http1AssignHeader dispatches a folded header name to parsedHTTPRequest slots (0 allocs).
func http1AssignHeader(req *parsedHTTPRequest, key, val []byte, hFlags *uint8, clValue *int) error {
	kl := len(key)
	if kl < 6 {
		return nil
	}
	_ = key[kl-1]

	switch kl {
	case 6: // accept
		if foldKeyU32(key, 0) == 0x65636361 && httpFold[key[4]] == 'p' && httpFold[key[5]] == 't' {
			req.Accept = val
		}
	case 9: // x-real-ip | sec-ch-ua
		switch foldKeyU32(key, 0) {
		case 0x65722d78: // x-re
			if httpFold[key[4]] == 'a' && httpFold[key[5]] == 'l' && httpFold[key[6]] == '-' &&
				httpFold[key[7]] == 'i' && httpFold[key[8]] == 'p' {
				if len(req.ClientIP) == 0 {
					req.ClientIP = val
				}
			}
		case 0x2d636573: // sec-
			if foldKeyU32(key, 4) == 0x752d6863 && httpFold[key[8]] == 'a' {
				req.SecCHUA = val
			}
		}
	case 10: // user-agent | x-tls-hash
		switch foldKeyU32(key, 0) {
		case 0x72657375: // user
			if foldKeyU32(key, 4) == 0x6567612d && httpFold[key[8]] == 'n' && httpFold[key[9]] == 't' {
				req.UserAgent = val
			}
		case 0x6c742d78: // x-tl
			if foldKeyU32(key, 4) == 0x61682d73 && httpFold[key[8]] == 's' && httpFold[key[9]] == 'h' {
				req.TLSHash = val
			}
		}
	case 12: // content-type
		if foldKeyU64(key, 0) == 0x2d746e65746e6f63 && foldKeyU32(key, 8) == 0x65707974 {
			req.ContentType = val
		}
	case 14: // content-length
		if foldKeyU64(key, 0) == 0x2d746e65746e6f63 && foldKeyU32(key, 8) == 0x676e656c &&
			httpFold[key[12]] == 't' && httpFold[key[13]] == 'h' {
			cl, ok := parseContentLengthStrict(val)
			if !ok {
				return errInvalidRequest
			}
			if *hFlags&http1flCLSet != 0 && *clValue != cl {
				return errInvalidRequest
			}
			*hFlags |= http1flCLSet
			*clValue = cl
			req.ContentLength = cl
			req.HasContentLength = true
		}
	case 15: // x-forwarded-for | accept-language
		switch foldKeyU32(key, 0) {
		case 0x6f662d78: // x-fo
			if foldKeyU64(key, 4) == 0x2d64656472617772 && httpFold[key[12]] == 'f' &&
				httpFold[key[13]] == 'o' && httpFold[key[14]] == 'r' {
				req.ClientIP = val
			}
		case 0x65636361: // acce
			if foldKeyU64(key, 4) == 0x75676e616c2d7470 && httpFold[key[12]] == 'a' &&
				httpFold[key[13]] == 'g' && httpFold[key[14]] == 'e' {
				req.AcceptLang = val
			}
		}
	case 17: // transfer-encoding
		if foldKeyU64(key, 0) == 0x726566736e617274 && foldKeyU64(key, 8) == 0x6e69646f636e652d &&
			httpFold[key[16]] == 'g' {
			*hFlags |= http1flHasTE
			if teValueOnlyChunked(val) {
				*hFlags |= http1flChunkedTE
			} else {
				*hFlags |= http1flInvalidTE
			}
		}
	}
	return nil
}

package ingestion

// http2_hpack.go — subset HPACK static-table decoder for /track ingress (M5-C2).

type h2StaticEntry struct {
	name  string
	value string
}

// h2StaticTable is RFC 7541 static table (index 1..N).
var h2StaticTable = []h2StaticEntry{
	{":authority", ""},
	{":method", "GET"},
	{":method", "POST"},
	{":path", "/"},
	{":path", "/index.html"},
	{":scheme", "http"},
	{":scheme", "https"},
	{":status", "200"},
	{":status", "204"},
	{":status", "206"},
	{":status", "304"},
	{":status", "400"},
	{":status", "404"},
	{":status", "500"},
	{"accept-charset", ""},
	{"accept-encoding", "gzip, deflate"},
	{"accept-language", ""},
	{"accept-ranges", "bytes"},
	{"accept", ""},
	{"access-control-allow-origin", ""},
	{"age", ""},
	{"allow", ""},
	{"authorization", ""},
	{"cache-control", ""},
	{"content-disposition", ""},
	{"content-encoding", ""},
	{"content-language", ""},
	{"content-length", ""},
	{"content-location", ""},
	{"content-range", ""},
	{"content-type", "text/html; charset=utf-8"},
	{"content-type", "text/plain; charset=utf-8"},
	{"content-type", "application/json"},
	{"cookie", ""},
	{"date", ""},
	{"etag", ""},
	{"expect", ""},
	{"expires", ""},
	{"from", ""},
	{"host", ""},
	{"if-match", ""},
	{"if-modified-since", ""},
	{"if-none-match", ""},
	{"if-range", ""},
	{"if-unmodified-since", ""},
	{"last-modified", ""},
	{"link", ""},
	{"location", ""},
	{"max-forwards", ""},
	{"proxy-authenticate", ""},
	{"proxy-authorization", ""},
	{"range", ""},
	{"referer", ""},
	{"refresh", ""},
	{"retry-after", ""},
	{"server", ""},
	{"set-cookie", ""},
	{"strict-transport-security", ""},
	{"transfer-encoding", ""},
	{"user-agent", ""},
	{"vary", ""},
	{"via", ""},
	{"www-authenticate", ""},
}

var (
	h2StaticNameB  [][]byte
	h2StaticValueB [][]byte
)

func init() {
	n := len(h2StaticTable)
	h2StaticNameB = make([][]byte, n)
	h2StaticValueB = make([][]byte, n)
	for i, e := range h2StaticTable {
		h2StaticNameB[i] = []byte(e.name)
		h2StaticValueB[i] = []byte(e.value)
	}
}

func h2StaticNameValue(index int) (name, value []byte, ok bool) {
	if index < 1 || index > len(h2StaticTable) {
		return nil, nil, false
	}
	i := index - 1
	return h2StaticNameB[i], h2StaticValueB[i], true
}

func h2DecodeString(data []byte, off int) (val []byte, next int, err error) {
	n := len(data)
	if off >= n {
		return nil, 0, errIncompleteRequest
	}
	_ = data[n-1]
	huff := data[off]&0x80 != 0
	strLen, off, err := h2DecodeInt(data, off, 7, 0x7f)
	if err != nil {
		return nil, 0, err
	}
	if strLen < 0 || off+strLen > n {
		return nil, 0, errIncompleteRequest
	}
	raw := data[off : off+strLen]
	off += strLen
	if huff {
		return nil, 0, errInvalidRequest
	}
	return raw, off, nil
}

// h2DecodeHeadersBlock decodes a HEADERS frame block into parsedHTTPRequest (static + literal, no dynamic table).
func h2DecodeHeadersBlock(block []byte, req *parsedHTTPRequest) error {
	var hFlags uint8
	var clValue int
	off := 0
	n := len(block)
	for off < n {
		_ = block[n-1]
		b := block[off]
		if b&0x80 != 0 {
			idx, next, err := h2DecodeInt(block, off, 7, 0x7f)
			if err != nil {
				return err
			}
			off = next
			name, val, ok := h2StaticNameValue(idx)
			if !ok {
				return errInvalidRequest
			}
			if err := h2AssignHeader(req, name, val, &hFlags, &clValue); err != nil {
				return err
			}
			continue
		}
		if b&0x40 != 0 {
			return errInvalidRequest
		}
		if b&0x20 != 0 {
			return errInvalidRequest
		}
		if b&0x10 != 0 {
			nameIdx, next, err := h2DecodeInt(block, off, 4, 0x0f)
			if err != nil {
				return err
			}
			off = next
			var name []byte
			if nameIdx > 0 {
				var ok bool
				name, _, ok = h2StaticNameValue(nameIdx)
				if !ok {
					return errInvalidRequest
				}
			} else {
				name, off, err = h2DecodeString(block, off)
				if err != nil {
					return err
				}
			}
			var val []byte
			val, off, err = h2DecodeString(block, off)
			if err != nil {
				return err
			}
			if err := h2AssignHeader(req, name, val, &hFlags, &clValue); err != nil {
				return err
			}
			continue
		}
		if b&0x80 == 0 && b&0x40 == 0 && b&0x20 == 0 && b&0x10 == 0 {
			nameIdx, next, err := h2DecodeInt(block, off, 4, 0x0f)
			if err != nil {
				return err
			}
			off = next
			var name []byte
			if nameIdx > 0 {
				var ok bool
				name, _, ok = h2StaticNameValue(nameIdx)
				if !ok {
					return errInvalidRequest
				}
			} else {
				name, off, err = h2DecodeString(block, off)
				if err != nil {
					return err
				}
			}
			var val []byte
			val, off, err = h2DecodeString(block, off)
			if err != nil {
				return err
			}
			if err := h2AssignHeader(req, name, val, &hFlags, &clValue); err != nil {
				return err
			}
			continue
		}
		return errInvalidRequest
	}
	if hFlags&http1flHasTE != 0 {
		return errInvalidRequest
	}
	return nil
}

func h2AssignHeader(req *parsedHTTPRequest, key, val []byte, hFlags *uint8, clValue *int) error {
	if len(key) > 0 && key[0] == ':' {
		switch len(key) {
		case 7:
			if bytesEqual(key, ":method") {
				req.Method = val
				return nil
			}
		case 5:
			if bytesEqual(key, ":path") {
				req.Path = val
				if len(req.Method) > 0 && !http1IngressValid(req.Method, req.Path) {
					return errInvalidRequest
				}
				return nil
			}
		}
		if bytesEqual(key, ":authority") || bytesEqual(key, ":scheme") {
			return nil
		}
		return errInvalidRequest
	}
	var folded [http1MaxHeaderNameLen]byte
	if len(key) > len(folded) {
		return errInvalidRequest
	}
	for i, c := range key {
		folded[i] = httpFold[c]
	}
	return http1AssignHeader(req, folded[:len(key)], val, hFlags, clValue)
}

func h2EncodeString(dst []byte, off int, val []byte) int {
	off = h2EncodeInt(dst, off, len(val), 7, 0x7f)
	if off+len(val) > len(dst) {
		return off
	}
	copy(dst[off:], val)
	return off + len(val)
}

package ingestion

import (
	"errors"

	"github.com/google/uuid"
)

var (
	errMalformedJSON = errors.New("malformed json")
)

// ParseTrackRequestJSON parses a TrackRequest via a schema-specific DFA with BCE.
func ParseTrackRequestJSON(v *TrackRequest, data []byte) error {
	return parseTrackRequestJSON(v, data)
}

func skipJSONValue(data []byte, start int) (int, error) {
	n := len(data)
	if start >= n {
		return start, errMalformedJSON
	}
	_ = data[n-1] // BCE hint

	i := start
	b := data[i]
	switch b {
	case '"':
		i++
		for i < n && data[i] != '"' {
			if data[i] == '\\' {
				i += 2
			} else {
				i++
			}
		}
		if i >= n {
			return i, errMalformedJSON
		}
		i++ // skip '"'
		return i, nil
	case '{', '[':
		depth := 1
		openChar := b
		closeChar := byte('}')
		if b == '[' {
			closeChar = byte(']')
		}

		i++
		inString := false
		for i < n && depth > 0 {
			char := data[i]
			if inString {
				if char == '"' {
					inString = false
				} else if char == '\\' {
					if i+1 < n {
						i++
					}
				}
			} else {
				switch char {
				case '"':
					inString = true
				case openChar:
					depth++
				case closeChar:
					depth--
				}
			}
			i++
		}
		if depth > 0 {
			return i, errMalformedJSON
		}
		return i, nil
	case 't', 'f', 'n': // true, false, null
		for i < n && !isDelimiter(data[i]) {
			i++
		}
		return i, nil
	default: // number
		for i < n && !isDelimiter(data[i]) {
			i++
		}
		return i, nil
	}
}

var jsonDelimiter [256]byte
var hexLookup [256]byte

func init() {
	jsonDelimiter[','] = 1
	jsonDelimiter['}'] = 1
	jsonDelimiter[']'] = 1
	jsonDelimiter[' '] = 1
	jsonDelimiter['\t'] = 1
	jsonDelimiter['\n'] = 1
	jsonDelimiter['\r'] = 1

	for i := range hexLookup {
		hexLookup[i] = 0xff
	}
	for i := byte('0'); i <= '9'; i++ {
		hexLookup[i] = i - '0'
	}
	for i := byte('a'); i <= 'f'; i++ {
		hexLookup[i] = i - 'a' + 10
	}
	for i := byte('A'); i <= 'F'; i++ {
		hexLookup[i] = i - 'A' + 10
	}
}

func isDelimiter(b byte) bool {
	return jsonDelimiter[b] != 0
}

// ParseUUID parses a 16-byte raw or 36-byte string UUID into dst without allocations.
func ParseUUID(b []byte, dst *uuid.UUID) bool {
	if len(b) == 16 {
		copy(dst[:], b)
		return true
	}
	if len(b) != 36 {
		return false
	}
	if b[8] != '-' || b[13] != '-' || b[18] != '-' || b[23] != '-' {
		return false
	}

	decode := func(h, l byte) (byte, bool) {
		vh := hexLookup[h]
		vl := hexLookup[l]
		if vh == 0xff || vl == 0xff {
			return 0, false
		}
		return (vh << 4) | vl, true
	}

	var ok bool
	dst[0], ok = decode(b[0], b[1])
	if !ok {
		return false
	}
	dst[1], ok = decode(b[2], b[3])
	if !ok {
		return false
	}
	dst[2], ok = decode(b[4], b[5])
	if !ok {
		return false
	}
	dst[3], ok = decode(b[6], b[7])
	if !ok {
		return false
	}

	dst[4], ok = decode(b[9], b[10])
	if !ok {
		return false
	}
	dst[5], ok = decode(b[11], b[12])
	if !ok {
		return false
	}

	dst[6], ok = decode(b[14], b[15])
	if !ok {
		return false
	}
	dst[7], ok = decode(b[16], b[17])
	if !ok {
		return false
	}

	dst[8], ok = decode(b[19], b[20])
	if !ok {
		return false
	}
	dst[9], ok = decode(b[21], b[22])
	if !ok {
		return false
	}

	dst[10], ok = decode(b[24], b[25])
	if !ok {
		return false
	}
	dst[11], ok = decode(b[26], b[27])
	if !ok {
		return false
	}
	dst[12], ok = decode(b[28], b[29])
	if !ok {
		return false
	}
	dst[13], ok = decode(b[30], b[31])
	if !ok {
		return false
	}
	dst[14], ok = decode(b[32], b[33])
	if !ok {
		return false
	}
	dst[15], ok = decode(b[34], b[35])
	return ok
}

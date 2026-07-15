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
	v.Reset()
	if len(data) == 0 {
		return errMalformedJSON
	}

	// BCE hint: single bounds check for the entire slice
	_ = data[len(data)-1]

	n := len(data)
	i := 0

	// Skip leading whitespace
	for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
		i++
	}

	if i >= n || data[i] != '{' {
		return errMalformedJSON
	}
	i++

	for i < n {
		// Skip whitespace
		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
			i++
		}
		if i >= n {
			return errMalformedJSON
		}

		if data[i] == '}' {
			return nil
		}

		if data[i] != '"' {
			return errMalformedJSON
		}
		i++

		// Parse key
		keyStart := i
		for i < n && data[i] != '"' {
			i++
		}
		if i >= n {
			return errMalformedJSON
		}
		keyEnd := i
		i++ // skip '"'

		key := data[keyStart:keyEnd]

		// Skip whitespace and expect ':'
		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
			i++
		}
		if i >= n || data[i] != ':' {
			return errMalformedJSON
		}
		i++ // skip ':'

		// Skip whitespace before value
		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
			i++
		}
		if i >= n {
			return errMalformedJSON
		}

		// Fast key matching using length and characters (Perfect hashing / Switch-Trie)
		isCampaignID := false
		isUserID := false
		isType := false
		isClickID := false
		isPayload := false

		switch len(key) {
		case 4: // "type"
			if key[0] == 't' && key[1] == 'y' && key[2] == 'p' && key[3] == 'e' {
				isType = true
			}
		case 7: // "payload" or "user_id"
			if key[0] == 'p' && key[1] == 'a' && key[2] == 'y' && key[3] == 'l' && key[4] == 'o' && key[5] == 'a' && key[6] == 'd' {
				isPayload = true
			} else if key[0] == 'u' && key[1] == 's' && key[2] == 'e' && key[3] == 'r' && key[4] == '_' && key[5] == 'i' && key[6] == 'd' {
				isUserID = true
			}
		case 8: // "click_id"
			if key[0] == 'c' && key[1] == 'l' && key[2] == 'i' && key[3] == 'c' && key[4] == 'k' && key[5] == '_' && key[6] == 'i' && key[7] == 'd' {
				isClickID = true
			}
		case 11: // "campaign_id"
			if key[0] == 'c' && key[1] == 'a' && key[2] == 'm' && key[3] == 'p' && key[4] == 'a' && key[5] == 'i' && key[6] == 'g' && key[7] == 'n' && key[8] == '_' && key[9] == 'i' && key[10] == 'd' {
				isCampaignID = true
			}
		}

		// Parse value
		if isCampaignID || isUserID || isType || isClickID {
			if data[i] != '"' {
				return errMalformedJSON
			}
			i++
			valStart := i
			for i < n && data[i] != '"' {
				if data[i] == '\\' {
					i += 2
				} else {
					i++
				}
			}
			if i >= n {
				return errMalformedJSON
			}
			valEnd := i
			i++ // skip '"'

			valBytes := data[valStart:valEnd]
			if isCampaignID {
				if !ParseUUID(valBytes, &v.CampaignID) {
					return errMalformedJSON
				}
			} else if isUserID {
				v.UserID = unsafeString(valBytes)
			} else if isType {
				v.Type = unsafeString(valBytes)
			} else if isClickID {
				v.ClickID = unsafeString(valBytes)
			}
		} else if isPayload {
			valStart := i
			valEnd, err := skipJSONValue(data, i)
			if err != nil {
				return err
			}
			v.Payload = data[valStart:valEnd]
			i = valEnd
		} else {
			// Unknown key, skip its value
			valEnd, err := skipJSONValue(data, i)
			if err != nil {
				return err
			}
			i = valEnd
		}

		// Skip whitespace and expect ',' or '}'
		for i < n && (data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r') {
			i++
		}
		if i >= n {
			return errMalformedJSON
		}

		if data[i] == ',' {
			i++
			continue
		} else if data[i] == '}' {
			return nil
		} else {
			return errMalformedJSON
		}
	}

	return errMalformedJSON
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

func isDelimiter(b byte) bool {
	return b == ',' || b == '}' || b == ']' || b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

var hexLookup [256]byte

func init() {
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

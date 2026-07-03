package ads

import (
	"unsafe"
)

// keyID identifies a known TrackRequest JSON field without bool chains.
type keyID uint8

const (
	keyUnknown keyID = iota
	keyType
	keyUserID
	keyPayload
	keyClickID
	keyCampaignID
)

// Packed little-endian constants for fixed JSON keys (no per-byte && chains).
const (
	u32Type     uint32 = 0x65707974         // "type"
	u32Payl     uint32 = 0x6c796170         // "payl" — first 4 of "payload"
	u32User     uint32 = 0x72657375         // "user" — first 4 of "user_id"
	u64ClickID  uint64 = 0x64695f6b63696c63 // "click_id"
	u64Campaign uint64 = 0x6e676961706d6163 // "campaign" — first 8 of "campaign_id"
)

var jsonWhitespace [256]byte

func init() {
	jsonWhitespace[' '] = 1
	jsonWhitespace['\t'] = 1
	jsonWhitespace['\n'] = 1
	jsonWhitespace['\r'] = 1
}

func loadU32(b []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&b[0]))
}

func loadU64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}

func skipJSONWS(data []byte, i, n int) int {
	for i < n && jsonWhitespace[data[i]] != 0 {
		i++
	}
	return i
}

// matchTrackKey maps a JSON object key slice to keyID using length + packed compares.
func matchTrackKey(key []byte) keyID {
	switch len(key) {
	case 4:
		if loadU32(key) == u32Type {
			return keyType
		}
	case 7:
		switch loadU32(key) {
		case u32Payl:
			if key[4] == 'o' && key[5] == 'a' && key[6] == 'd' {
				return keyPayload
			}
		case u32User:
			if key[4] == '_' && key[5] == 'i' && key[6] == 'd' {
				return keyUserID
			}
		}
	case 8:
		if loadU64(key) == u64ClickID {
			return keyClickID
		}
	case 11:
		if loadU64(key) == u64Campaign && key[8] == '_' && key[9] == 'i' && key[10] == 'd' {
			return keyCampaignID
		}
	}
	return keyUnknown
}

// ParseTrackRequestJSONOpt is a lower-branch variant of ParseTrackRequestJSON for benchmarking.
func ParseTrackRequestJSONOpt(v *TrackRequest, data []byte) error {
	v.Reset()
	if len(data) == 0 {
		return errMalformedJSON
	}
	_ = data[len(data)-1]

	n := len(data)
	i := skipJSONWS(data, 0, n)
	if i >= n || data[i] != '{' {
		return errMalformedJSON
	}
	i++

	for i < n {
		i = skipJSONWS(data, i, n)
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

		keyStart := i
		for i < n && data[i] != '"' {
			i++
		}
		if i >= n {
			return errMalformedJSON
		}
		keyEnd := i
		i++

		i = skipJSONWS(data, i, n)
		if i >= n || data[i] != ':' {
			return errMalformedJSON
		}
		i++

		i = skipJSONWS(data, i, n)
		if i >= n {
			return errMalformedJSON
		}

		kid := matchTrackKey(data[keyStart:keyEnd])
		switch kid {
		case keyType, keyUserID, keyClickID:
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
			valBytes := data[valStart:i]
			i++

			switch kid {
			case keyType:
				v.Type = unsafeString(valBytes)
			case keyUserID:
				v.UserID = unsafeString(valBytes)
			case keyClickID:
				v.ClickID = unsafeString(valBytes)
			}
		case keyCampaignID:
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
			if !ParseUUID(data[valStart:i], &v.CampaignID) {
				return errMalformedJSON
			}
			i++
		case keyPayload:
			valStart := i
			valEnd, err := skipJSONValue(data, i)
			if err != nil {
				return err
			}
			v.Payload = data[valStart:valEnd]
			i = valEnd
		default:
			valEnd, err := skipJSONValue(data, i)
			if err != nil {
				return err
			}
			i = valEnd
		}

		i = skipJSONWS(data, i, n)
		if i >= n {
			return errMalformedJSON
		}
		switch data[i] {
		case ',':
			i++
		case '}':
			return nil
		default:
			return errMalformedJSON
		}
	}

	return errMalformedJSON
}

package ingestion

import (
	"espx/internal/campaignmodel"

	"github.com/google/uuid"
)

// OpenRTB 3.0 / AdCOM key IDs for the shared hot-path JSON FSM (M12-02 / M12-08).
type ortbKeyID uint8

const (
	ortbKeyUnknown ortbKeyID = iota
	ortbKeyOpenrtb
	ortbKeyRequest
	ortbKeyItem
	ortbKeyContext
	ortbKeyDevice
	ortbKeyFlr
	ortbKeyType
	ortbKeyIDField
	ortbKeyDealID
	ortbKeyCategoryMask
	ortbKeyTagid
)

const (
	ortbDealIDMax = 64
	ortbItemIDMax = 64
	ortbReqIDMax  = 64
	ortbMaxDepth  = OrtbMaxJSONDepth
)

// OpenRTB3Parsed holds fields extracted by the OpenRTB 3.0 / AdCOM JSON FSM (0 heap).
// String fields are offsets into the input payload (caller must keep payload live).
type OpenRTB3Parsed struct {
	MinBid       int64
	DeviceType   uint8
	CategoryMask uint64
	DealIDOff    int
	DealIDLen    uint8
	ItemIDOff    int
	ItemIDLen    uint8
	RequestIDOff int
	RequestIDLen uint8
	TagIDOff     int
	TagIDLen     uint8
	IsOpenRTB    bool
	OK           bool
}

// Packed little-endian constants for OpenRTB / AdCOM JSON keys.
const (
	u32OrtbFlr      uint32 = 0x726c66           // "flr" (len 3 uses byte compare)
	u32OrtbType     uint32 = 0x65707974         // "type"
	u32OrtbItem     uint32 = 0x6d657469         // "item"
	u32OrtbTagid    uint32 = 0x69676174         // "tagi", first 4 of "tagid"
	u32OrtbOpen     uint32 = 0x6e65706f         // "open", first 4 of "openrtb"
	u32OrtbReq      uint32 = 0x75716572         // "requ", first 4 of "request"
	u32OrtbCont     uint32 = 0x746e6f63         // "cont", first 4 of "context"
	u32OrtbDeal     uint32 = 0x6c616564         // "deal", first 4 of "deal_id"
	u64OrtbCategory uint64 = 0x79726f6765746163 // "category", first 8 of "category_mask"
)

// matchOrtbKey maps a JSON object key to ortbKeyID via length + packed compares.
func matchOrtbKey(key []byte) ortbKeyID {
	switch len(key) {
	case 2:
		if key[0] == 'i' && key[1] == 'd' {
			return ortbKeyIDField
		}
	case 3:
		if key[0] == 'f' && key[1] == 'l' && key[2] == 'r' {
			return ortbKeyFlr
		}
	case 4:
		switch loadU32(key) {
		case u32OrtbType:
			return ortbKeyType
		case u32OrtbItem:
			return ortbKeyItem
		}
	case 5:
		if loadU32(key) == u32OrtbTagid && key[4] == 'd' {
			return ortbKeyTagid
		}
	case 6:
		if loadU32(key) == 0x69766564 && key[4] == 'c' && key[5] == 'e' {
			return ortbKeyDevice
		}
	case 7:
		switch loadU32(key) {
		case u32OrtbOpen:
			if key[4] == 'r' && key[5] == 't' && key[6] == 'b' {
				return ortbKeyOpenrtb
			}
		case u32OrtbReq:
			if key[4] == 'e' && key[5] == 's' && key[6] == 't' {
				return ortbKeyRequest
			}
		case u32OrtbCont:
			if key[4] == 'e' && key[5] == 'x' && key[6] == 't' {
				return ortbKeyContext
			}
		case u32OrtbDeal:
			if key[4] == '_' && key[5] == 'i' && key[6] == 'd' {
				return ortbKeyDealID
			}
		}
	case 13:
		if loadU64(key) == u64OrtbCategory && key[8] == '_' &&
			key[9] == 'm' && key[10] == 'a' && key[11] == 's' && key[12] == 'k' {
			return ortbKeyCategoryMask
		}
	}
	return ortbKeyUnknown
}

type ortbFrame struct {
	parent  ortbKeyID
	inArray bool
	itemIdx int // 0-based index within item array; -1 if not in item array
}

// parseOpenRTB3FSM walks OpenRTB 3.0 / AdCOM JSON incrementally (no bytes.Index, 0 allocs).
func parseOpenRTB3FSM(payload []byte) OpenRTB3Parsed {
	var out OpenRTB3Parsed
	parseOpenRTB3FSMInto(&out, payload)
	return out
}

func parseOrtbObject(data []byte, i, n int, out *OpenRTB3Parsed, stack *[ortbMaxDepth]ortbFrame, depth *int) (int, bool) {
	if i >= n || data[i] != '{' {
		return i, false
	}
	i++
	frame := stack[*depth]

	for i < n {
		i = skipJSONWS(data, i, n)
		if i >= n {
			return i, false
		}
		if data[i] == '}' {
			return i + 1, true
		}
		if data[i] != '"' {
			return i, false
		}
		i++
		keyStart := i
		for i < n && data[i] != '"' {
			if data[i] == '\\' {
				i++
			}
			i++
		}
		if i >= n {
			return i, false
		}
		key := data[keyStart:i]
		i++
		i = skipJSONWS(data, i, n)
		if i >= n || data[i] != ':' {
			return i, false
		}
		i++
		i = skipJSONWS(data, i, n)
		if i >= n {
			return i, false
		}

		kid := matchOrtbKey(key)
		if kid == ortbKeyOpenrtb {
			out.IsOpenRTB = true
		}

		switch data[i] {
		case '{':
			if *depth+1 >= ortbMaxDepth {
				return i, false
			}
			*depth++
			stack[*depth] = ortbFrame{parent: kid, itemIdx: frame.itemIdx}
			var ok bool
			i, ok = parseOrtbObject(data, i, n, out, stack, depth)
			*depth--
			if !ok {
				return i, false
			}
		case '[':
			i++
			if kid == ortbKeyItem {
				out.IsOpenRTB = true
				itemIdx := 0
				for i < n {
					i = skipJSONWS(data, i, n)
					if i >= n {
						return i, false
					}
					if data[i] == ']' {
						i++
						break
					}
					if data[i] == '{' {
						if *depth+1 >= ortbMaxDepth {
							return i, false
						}
						*depth++
						stack[*depth] = ortbFrame{parent: ortbKeyItem, inArray: true, itemIdx: itemIdx}
						var ok bool
						i, ok = parseOrtbObject(data, i, n, out, stack, depth)
						*depth--
						if !ok {
							return i, false
						}
						itemIdx++
					} else {
						var err bool
						i, err = skipJSONValueAt(data, i, n)
						if err {
							return i, false
						}
					}
					i = skipJSONWS(data, i, n)
					if i < n && data[i] == ',' {
						i++
					}
				}
			} else {
				var err bool
				i, err = skipJSONArrayFrom(data, i, n)
				if err {
					return i, false
				}
			}
		case '"':
			i++
			valStart := i
			for i < n && data[i] != '"' {
				if data[i] == '\\' {
					i++
				}
				i++
			}
			if i >= n {
				return i, false
			}
			applyOrtbString(out, kid, frame, valStart, i)
			i++
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			valStart := i
			if data[i] == '-' {
				i++
			}
			for i < n && ((data[i] >= '0' && data[i] <= '9') || data[i] == '.') {
				i++
			}
			applyOrtbNumber(out, kid, frame, data[valStart:i])
		default:
			var err bool
			i, err = skipJSONValueAt(data, i, n)
			if err {
				return i, false
			}
		}

		i = skipJSONWS(data, i, n)
		if i < n && data[i] == ',' {
			i++
			continue
		}
		if i < n && data[i] == '}' {
			return i + 1, true
		}
	}
	return i, false
}

func applyOrtbString(out *OpenRTB3Parsed, kid ortbKeyID, frame ortbFrame, valStart, valEnd int) {
	ln := valEnd - valStart
	if ln <= 0 {
		return
	}
	switch kid {
	case ortbKeyDealID:
		if ln > ortbDealIDMax {
			ln = ortbDealIDMax
		}
		out.DealIDOff = valStart
		out.DealIDLen = uint8(ln)
	case ortbKeyTagid:
		if ln > ortbItemIDMax {
			ln = ortbItemIDMax
		}
		out.TagIDOff = valStart
		out.TagIDLen = uint8(ln)
	case ortbKeyIDField:
		switch {
		case frame.parent == ortbKeyRequest:
			if ln > ortbReqIDMax {
				ln = ortbReqIDMax
			}
			out.RequestIDOff = valStart
			out.RequestIDLen = uint8(ln)
		case frame.parent == ortbKeyItem && frame.itemIdx == 0 && out.ItemIDLen == 0:
			if ln > ortbItemIDMax {
				ln = ortbItemIDMax
			}
			out.ItemIDOff = valStart
			out.ItemIDLen = uint8(ln)
		}
	}
}

func applyOrtbNumber(out *OpenRTB3Parsed, kid ortbKeyID, frame ortbFrame, val []byte) {
	switch kid {
	case ortbKeyFlr:
		if out.MinBid == 0 {
			out.MinBid = parseDecimalMicro(val)
		}
	case ortbKeyType:
		if frame.parent == ortbKeyDevice {
			var adcomType int64
			for j := 0; j < len(val); j++ {
				c := val[j]
				if c >= '0' && c <= '9' {
					adcomType = adcomType*10 + int64(c-'0')
				}
			}
			out.DeviceType = openrtbDeviceType(adcomType)
		}
	case ortbKeyCategoryMask:
		var mask uint64
		for j := 0; j < len(val); j++ {
			c := val[j]
			if c >= '0' && c <= '9' {
				mask = mask*10 + uint64(c-'0')
			}
		}
		if mask != 0 {
			out.CategoryMask = mask
		}
	}
}

// ortbSlice returns a subslice of payload for an offset/len pair.
func ortbSlice(payload []byte, off int, ln uint8) []byte {
	if ln == 0 || off < 0 || off+int(ln) > len(payload) {
		return nil
	}
	return payload[off : off+int(ln)]
}

func skipJSONValueAt(data []byte, i, n int) (int, bool) {
	if i >= n {
		return i, true
	}
	switch data[i] {
	case '"':
		i++
		for i < n && data[i] != '"' {
			if data[i] == '\\' {
				i++
			}
			i++
		}
		if i >= n {
			return i, true
		}
		return i + 1, false
	case '{':
		return skipJSONObjectFrom(data, i+1, n)
	case '[':
		return skipJSONArrayFrom(data, i+1, n)
	case 't': // true
		if i+3 < n {
			return i + 4, false
		}
		return i, true
	case 'f': // false
		if i+4 < n {
			return i + 5, false
		}
		return i, true
	case 'n': // null
		if i+3 < n {
			return i + 4, false
		}
		return i, true
	default:
		for i < n && data[i] != ',' && data[i] != '}' && data[i] != ']' && jsonWhitespace[data[i]] == 0 {
			i++
		}
		return i, false
	}
}

func skipJSONObjectFrom(data []byte, i, n int) (int, bool) {
	depth := 1
	for i < n && depth > 0 {
		c := data[i]
		if c == '"' {
			i++
			for i < n && data[i] != '"' {
				if data[i] == '\\' {
					i++
				}
				i++
			}
			if i >= n {
				return i, true
			}
			i++
			continue
		}
		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
		}
		i++
	}
	return i, depth != 0
}

func skipJSONArrayFrom(data []byte, i, n int) (int, bool) {
	depth := 1
	for i < n && depth > 0 {
		c := data[i]
		if c == '"' {
			i++
			for i < n && data[i] != '"' {
				if data[i] == '\\' {
					i++
				}
				i++
			}
			if i >= n {
				return i, true
			}
			i++
			continue
		}
		if c == '[' {
			depth++
		} else if c == ']' {
			depth--
		}
		i++
	}
	return i, depth != 0
}

// ParseOpenRTB3Ingress parses a /track OpenRTB 3.0 body into TrackRequest fields (0 allocs).
// String fields alias into data; caller must keep data live for the request lifetime.
// Parsed OpenRTB fields are cached on dst.ortbSlot for reuse by buildRtbTargeting.
func ParseOpenRTB3Ingress(dst *TrackRequest, data []byte) error {
	if dst == nil {
		return errMalformedJSON
	}
	dst.resetForParse()
	if dst.ortbSlot != nil {
		releaseOpenRTBScratchSlot(dst.ortbSlot)
		dst.ortbSlot = nil
	}
	slot := acquireOpenRTBScratchSlot()
	if !parseOpenRTB3FSMInto(&slot.parsed, data) {
		releaseOpenRTBScratchSlot(slot)
		return errMalformedJSON
	}
	parsed := slot.parsed
	dst.ortbSlot = slot
	item := ortbSlice(data, parsed.ItemIDOff, parsed.ItemIDLen)
	if len(item) == 0 || !ParseUUID(item, &dst.CampaignID) {
		releaseOpenRTBScratchSlot(slot)
		dst.ortbSlot = nil
		return errMalformedJSON
	}
	if parsed.RequestIDLen > 0 {
		dst.ClickID = unsafeString(ortbSlice(data, parsed.RequestIDOff, parsed.RequestIDLen))
	}
	if parsed.TagIDLen > 0 {
		dst.PlacementID = unsafeString(ortbSlice(data, parsed.TagIDOff, parsed.TagIDLen))
	}
	dst.Type = "impression"
	dst.Payload = data
	return nil
}

// ApplyOpenRTB3ToEvent maps parsed OpenRTB 3 fields onto a pooled Event.
func ApplyOpenRTB3ToEvent(evt *campaignmodel.Event, data []byte, parsed *OpenRTB3Parsed) bool {
	if evt == nil || parsed == nil || !parsed.OK {
		return false
	}
	var id uuid.UUID
	item := ortbSlice(data, parsed.ItemIDOff, parsed.ItemIDLen)
	if len(item) == 0 || !ParseUUID(item, &id) {
		return false
	}
	evt.CampaignID = id
	if parsed.RequestIDLen > 0 {
		evt.ClickID = unsafeString(ortbSlice(data, parsed.RequestIDOff, parsed.RequestIDLen))
	}
	if parsed.TagIDLen > 0 {
		evt.PlacementID = unsafeString(ortbSlice(data, parsed.TagIDOff, parsed.TagIDLen))
	}
	if evt.Type == "" {
		evt.Type = "impression"
	}
	evt.Payload = append(evt.Payload[:0], data...)
	return true
}

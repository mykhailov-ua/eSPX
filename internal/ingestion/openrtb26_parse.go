package ingestion

import (
	"bytes"
	"time"
)

const (
	openrtb26DealIDMax = 64
	openrtb26SeatMax   = 8
)

var (
	openrtbKeyImp        = []byte(`"imp"`)
	openrtbKeyTmax       = []byte(`"tmax"`)
	openrtbKeyBidfloor   = []byte(`"bidfloor"`)
	openrtbKeyDevicetype = []byte(`"devicetype"`)
	openrtbKeyCat        = []byte(`"cat"`)
	openrtbKeyWseat      = []byte(`"wseat"`)
	openrtbKeyDeals      = []byte(`"deals"`)
	openrtbKeyPmp        = []byte(`"pmp"`)
	openrtbKeyID         = []byte(`"id"`)
	openrtbKeySchain     = []byte(`"schain"`)
	openrtbKeyNodes      = []byte(`"nodes"`)
	openrtbKeyAsi        = []byte(`"asi"`)
	openrtbKeySid        = []byte(`"sid"`)
)

// OpenRTB26Parsed carries hot-path bid request fields parsed without heap allocation.
type OpenRTB26Parsed struct {
	BidFloorMicro int64
	DeviceType    uint8
	CategoryMask  uint64
	SeatCount     uint8
	TmaxMs        int32
	DealID        [openrtb26DealIDMax]byte
	DealIDLen     uint8
	Schain        SchainNodes
	OK            bool
}

// ParseOpenRTB26 parses OpenRTB 2.6 bid request JSON on the hot path with zero heap allocations.
func ParseOpenRTB26(payload []byte) OpenRTB26Parsed {
	var out OpenRTB26Parsed
	n := len(payload)
	if n < 12 || !bytes.Contains(payload, openrtbKeyImp) {
		return out
	}
	out.OK = true
	out.DeviceType = 1
	out.CategoryMask = 1
	out.TmaxMs = 200
	out.SeatCount = 1

	if idx := bytes.Index(payload, openrtbKeyTmax); idx >= 0 {
		out.TmaxMs = int32(parseJSONIntField(payload, idx+len(openrtbKeyTmax)))
		if out.TmaxMs <= 0 {
			out.TmaxMs = 200
		}
	}
	if idx := bytes.Index(payload, openrtbKeyBidfloor); idx >= 0 {
		out.BidFloorMicro = parseDecimalMicroField(payload, idx+len(openrtbKeyBidfloor))
	}
	if idx := bytes.Index(payload, openrtbKeyDevicetype); idx >= 0 {
		out.DeviceType = openrtbDeviceType(parseJSONIntField(payload, idx+len(openrtbKeyDevicetype)))
	}
	if idx := bytes.Index(payload, openrtbKeyCat); idx >= 0 {
		out.CategoryMask = parseCategoryMaskFromArray(payload, idx)
	}
	if idx := bytes.Index(payload, openrtbKeyWseat); idx >= 0 {
		out.SeatCount = parseSeatCountAt(payload, idx)
	}
	searchFrom := bytes.Index(payload, openrtbKeyDeals)
	if searchFrom < 0 {
		searchFrom = bytes.Index(payload, openrtbKeyPmp)
	}
	if searchFrom >= 0 {
		slice := payload[searchFrom:]
		if idRel := bytes.Index(slice, openrtbKeyID); idRel >= 0 {
			out.DealIDLen = uint8(parseQuotedField(payload, searchFrom+idRel+len(openrtbKeyID), out.DealID[:]))
		}
	}
	if idx := bytes.Index(payload, openrtbKeySchain); idx >= 0 {
		out.Schain = parseSchainNodesAt(payload, idx)
	}
	return out
}

func parseSchainNodesAt(payload []byte, schainAt int) SchainNodes {
	var out SchainNodes
	n := len(payload)
	nodesAt := bytes.Index(payload[schainAt:], openrtbKeyNodes)
	if nodesAt < 0 {
		return out
	}
	i := schainAt + nodesAt + len(openrtbKeyNodes)
	for i < n && payload[i] != '[' {
		i++
	}
	if i >= n {
		return out
	}
	i++
	for i < n && out.Count < schainNodeMax {
		if payload[i] == ']' {
			break
		}
		if payload[i] != '{' {
			i++
			continue
		}
		objEnd := i
		depth := 0
		for objEnd < n {
			if payload[objEnd] == '{' {
				depth++
			} else if payload[objEnd] == '}' {
				depth--
				if depth == 0 {
					break
				}
			}
			objEnd++
		}
		if objEnd >= n {
			break
		}
		obj := payload[i : objEnd+1]
		node := SchainNode{}
		if asiRel := bytes.Index(obj, openrtbKeyAsi); asiRel >= 0 {
			node.ASILen = uint8(parseQuotedField(obj, asiRel+len(openrtbKeyAsi), node.ASI[:]))
		}
		if sidRel := bytes.Index(obj, openrtbKeySid); sidRel >= 0 {
			node.SIDLen = uint8(parseQuotedField(obj, sidRel+len(openrtbKeySid), node.SID[:]))
		}
		out.Nodes[out.Count] = node
		out.Count++
		i = objEnd + 1
	}
	return out
}

func openrtbDeviceType(dt int64) uint8 {
	switch dt {
	case 1, 4:
		return 2
	case 2:
		return 1
	case 5:
		return 4
	default:
		return 1
	}
}

func parseSeatCountAt(payload []byte, wseatAt int) uint8 {
	n := len(payload)
	i := wseatAt + len(openrtbKeyWseat)
	for i < n && payload[i] != '[' {
		i++
	}
	if i >= n {
		return 1
	}
	i++
	count := uint8(0)
	for i < n && payload[i] != ']' && count < openrtb26SeatMax {
		if payload[i] == '"' {
			count++
			i++
			for i < n && payload[i] != '"' {
				i++
			}
		}
		i++
	}
	if count == 0 {
		return 1
	}
	return count
}

func parseQuotedField(payload []byte, start int, dst []byte) int {
	n := len(payload)
	i := start
	for i < n && (payload[i] == ' ' || payload[i] == '\t' || payload[i] == ':') {
		i++
	}
	if i >= n || payload[i] != '"' {
		return 0
	}
	i++
	fieldStart := i
	for i < n && payload[i] != '"' {
		i++
	}
	ln := i - fieldStart
	if ln <= 0 || ln > len(dst) {
		return 0
	}
	copy(dst[:ln], payload[fieldStart:i])
	return ln
}

// DeadlineMonoFromTmax converts OpenRTB tmax milliseconds to a monotonic auction deadline.
func DeadlineMonoFromTmax(tmaxMs int32) int64 {
	if tmaxMs <= 0 {
		tmaxMs = 200
	}
	return monotonicNano() + int64(tmaxMs)*int64(time.Millisecond)
}

func parseJSONIntField(payload []byte, start int) int64 {
	n := len(payload)
	i := start
	for i < n && (payload[i] == ' ' || payload[i] == '\t' || payload[i] == ':') {
		i++
	}
	var val int64
	for i < n && payload[i] >= '0' && payload[i] <= '9' {
		val = val*10 + int64(payload[i]-'0')
		i++
	}
	return val
}

func parseDecimalMicroField(payload []byte, start int) int64 {
	n := len(payload)
	i := start
	for i < n && (payload[i] == ' ' || payload[i] == '\t' || payload[i] == ':') {
		i++
	}
	valStart := i
	for i < n && ((payload[i] >= '0' && payload[i] <= '9') || payload[i] == '.') {
		i++
	}
	if valStart < i {
		return parseDecimalMicro(payload[valStart:i])
	}
	return 0
}

func parseCategoryMaskFromArray(payload []byte, catIdx int) uint64 {
	n := len(payload)
	i := catIdx + len(openrtbKeyCat)
	for i < n && payload[i] != '[' {
		i++
	}
	if i >= n {
		return 1
	}
	i++
	var mask uint64
	for i < n && payload[i] != ']' {
		if payload[i] == '"' {
			i++
			start := i
			for i < n && payload[i] != '"' {
				i++
			}
			if start < i && i-start > 0 {
				d := payload[i-1]
				if d >= '0' && d <= '9' {
					mask |= uint64(1) << uint64(d-'0')
				} else {
					mask |= 1
				}
			}
			i++
			continue
		}
		i++
	}
	if mask == 0 {
		return 1
	}
	return mask
}

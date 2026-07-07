package ads

import (
	"bytes"
)

// parseDecimalMicro parses a decimal string like "1.50" or "150" into micro-units (multiplied by 1,000,000).
func parseDecimalMicro(b []byte) int64 {
	i := 0
	n := len(b)
	for i < n && (b[i] == ' ' || b[i] == '\t') {
		i++
	}
	if i >= n {
		return 0
	}
	var val int64
	var dec int64
	var decDigits int
	hasDec := false
	for i < n {
		c := b[i]
		if c >= '0' && c <= '9' {
			if !hasDec {
				val = val*10 + int64(c-'0')
			} else {
				if decDigits < 6 {
					dec = dec*10 + int64(c-'0')
					decDigits++
				}
			}
		} else if c == '.' {
			hasDec = true
		} else {
			break
		}
		i++
	}
	// Pad decimal part to 6 digits (micro-units)
	for decDigits < 6 {
		dec *= 10
		decDigits++
	}
	return val*1000000 + dec
}

// ParseOpenRTB3Payload parses a nested OpenRTB 3.0 / AdCOM JSON payload on the hot path with 0 allocs.
func ParseOpenRTB3Payload(payload []byte) (minBid int64, deviceType uint8, categoryMask uint64, isOpenRTB bool) {
	n := len(payload)
	if n < 10 {
		return 0, 0, 0, false
	}

	openrtbIdx := bytes.Index(payload, []byte(`"openrtb"`))
	if openrtbIdx == -1 {
		return 0, 0, 0, false
	}
	isOpenRTB = true

	minBid = 0
	deviceType = 1
	categoryMask = 1

	flrIdx := bytes.Index(payload, []byte(`"flr"`))
	if flrIdx != -1 {
		idx := flrIdx + 5
		for idx < n && (payload[idx] == ' ' || payload[idx] == '\t' || payload[idx] == ':') {
			idx++
		}
		valStart := idx
		for idx < n && ((payload[idx] >= '0' && payload[idx] <= '9') || payload[idx] == '.') {
			idx++
		}
		if valStart < idx {
			minBid = parseDecimalMicro(payload[valStart:idx])
		}
	}

	deviceIdx := bytes.Index(payload, []byte(`"device"`))
	if deviceIdx != -1 {
		typeIdx := bytes.Index(payload[deviceIdx:], []byte(`"type"`))
		if typeIdx != -1 {
			idx := deviceIdx + typeIdx + 6
			for idx < n && (payload[idx] == ' ' || payload[idx] == '\t' || payload[idx] == ':') {
				idx++
			}
			valStart := idx
			for idx < n && (payload[idx] >= '0' && payload[idx] <= '9') {
				idx++
			}
			if valStart < idx {
				var adcomType int
				for j := valStart; j < idx; j++ {
					adcomType = adcomType*10 + int(payload[j]-'0')
				}
				// Map AdCOM 1.0 device types to our internal DeviceMask:
				// 1 = Mobile/Tablet, 2 = PC, 4 = Phone, 5 = Tablet
				switch adcomType {
				case 2: // PC -> Desktop (1)
					deviceType = 1
				case 4, 1: // Phone / Mobile -> Mobile (2)
					deviceType = 2
				case 5: // Tablet -> Tablet (4)
					deviceType = 4
				default:
					deviceType = 1
				}
			}
		}
	}

	catIdx := bytes.Index(payload, []byte(`"category_mask"`))
	if catIdx != -1 {
		idx := catIdx + 15
		for idx < n && (payload[idx] == ' ' || payload[idx] == '\t' || payload[idx] == ':') {
			idx++
		}
		valStart := idx
		for idx < n && (payload[idx] >= '0' && payload[idx] <= '9') {
			idx++
		}
		if valStart < idx {
			var mask uint64
			for j := valStart; j < idx; j++ {
				mask = mask*10 + uint64(payload[j]-'0')
			}
			categoryMask = mask
		}
	}

	return minBid, deviceType, categoryMask, true
}

// ParseDealID extracts a PMP deal_id from OpenRTB or legacy JSON payload without full unmarshaling.
func ParseDealID(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	key := []byte(`"deal_id"`)
	idx := bytes.Index(payload, key)
	if idx == -1 {
		return ""
	}
	i := idx + len(key)
	n := len(payload)
	for i < n && (payload[i] == ' ' || payload[i] == '\t' || payload[i] == ':') {
		i++
	}
	if i >= n || payload[i] != '"' {
		return ""
	}
	i++
	start := i
	for i < n && payload[i] != '"' {
		i++
	}
	if start >= i {
		return ""
	}
	return string(payload[start:i])
}

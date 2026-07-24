package ingestion

import (
	"espx/internal/campaignmodel"
)

// ensureIngestGeo resolves country and geo hash once per event for RTB and GeoFilter.
func ensureIngestGeo(geo GeoProvider, evt *campaignmodel.Event) {
	if geo == nil || evt == nil || evt.IP == "" || evt.IngestGeoResolved {
		return
	}
	evt.IngestGeoResolved = true
	country, err := geo.GetCountry(evt.IP)
	if err != nil || country == "" {
		return
	}
	evt.GeoCountry = country
	evt.GeoHash = GeoHashFromCountry(country)
}

// parseCategoryMask reads category_mask from JSON payloads without full unmarshaling.
func parseCategoryMask(payload []byte) uint64 {
	n := len(payload)
	if n < 15 {
		return 0
	}
	_ = payload[n-1]

	for i := 0; i <= n-15; i++ {
		if payload[i] != '"' || loadU64(payload[i+1:]) != 0x79726f6765746163 {
			continue
		}
		if payload[i+9] != '_' || payload[i+10] != 'm' || payload[i+11] != 'a' ||
			payload[i+12] != 's' || payload[i+13] != 'k' {
			continue
		}
		idx := i + 14
		if idx >= n || payload[idx] != '"' {
			continue
		}
		idx++
		for idx < n && (payload[idx] == ' ' || payload[idx] == '\t' || payload[idx] == ':') {
			if payload[idx] == ':' {
				idx++
				break
			}
			idx++
		}
		for idx < n && (payload[idx] == ' ' || payload[idx] == '\t') {
			idx++
		}
		var val uint64
		hasDigit := false
		for idx < n && payload[idx] >= '0' && payload[idx] <= '9' {
			val = val*10 + uint64(payload[idx]-'0')
			idx++
			hasDigit = true
		}
		if hasDigit {
			return val
		}
		return 0
	}
	return 0
}

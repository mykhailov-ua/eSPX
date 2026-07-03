package filter

import (
	"bytes"
	"hash/crc32"

	"espx/internal/domain"
)

var parseCategoryMaskKey = []byte(`"category_mask"`)

// ensureIngestGeo resolves country and geo hash once per event for RTB and GeoFilter.
// EnsureIngestGeo resolves ingest geo once per event when a provider is configured.
func EnsureIngestGeo(geo GeoProvider, evt *domain.Event) {
	ensureIngestGeo(geo, evt)
}

func ensureIngestGeo(geo GeoProvider, evt *domain.Event) {
	if geo == nil || evt == nil || evt.IP == "" || evt.IngestGeoResolved {
		return
	}
	evt.IngestGeoResolved = true
	country, err := geo.GetCountry(evt.IP)
	if err != nil || country == "" {
		return
	}
	evt.GeoCountry = country
	evt.GeoHash = geoHashFromCountry(country)
}

func geoHashFromCountry(country string) uint32 {
	if country == "" {
		return 0
	}
	return crc32.ChecksumIEEE([]byte(country))
}

// ParseCategoryMask reads category_mask from JSON payloads without full unmarshaling.
func ParseCategoryMask(payload []byte) uint64 {
	return parseCategoryMask(payload)
}

// parseCategoryMask reads category_mask from JSON payloads without full unmarshaling.
func parseCategoryMask(payload []byte) uint64 {
	n := len(payload)
	kLen := len(parseCategoryMaskKey)
	if n < kLen {
		return 0
	}
	for i := 0; i <= n-kLen; i++ {
		if payload[i] == '"' && bytes.Equal(payload[i:i+kLen], parseCategoryMaskKey) {
			idx := i + kLen
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
	}
	return 0
}

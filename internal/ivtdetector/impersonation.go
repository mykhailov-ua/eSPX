package ivtdetector

import "strings"

// IsTLSImpersonating reports UA/JA3 mismatches indicative of automated clients.
func IsTLSImpersonating(ua, ja3 string) bool {
	if ua == "" || ja3 == "" {
		return false
	}
	uaLower := strings.ToLower(ua)
	isChrome := strings.Contains(uaLower, "chrome") && !strings.Contains(uaLower, "chromium")
	isPython := strings.Contains(ja3, "python-requests") || ja3 == "37b37375c33a2e6a17b2b6400c436321"
	return isChrome && isPython
}

package auth

import "strings"

// parseBearerToken extracts the token from an Authorization bearer header value.
func parseBearerToken(header string) (token string, ok bool) {
	if len(header) < 7 || !strings.EqualFold(header[:7], "bearer ") {
		return "", false
	}
	token = strings.TrimSpace(header[7:])
	return token, token != ""
}

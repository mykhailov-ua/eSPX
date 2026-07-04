package auth

import "strings"

func parseBearerToken(header string) (token string, ok bool) {
	if len(header) < 7 || !strings.EqualFold(header[:7], "bearer ") {
		return "", false
	}
	token = strings.TrimSpace(header[7:])
	return token, token != ""
}

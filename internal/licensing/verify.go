package licensing

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

var (
	ErrInvalidTokenFormat = errors.New("invalid token format")
	ErrInvalidSignature   = errors.New("invalid signature")
	ErrTokenExpired       = errors.New("token is expired")
	ErrTokenNotYetValid   = errors.New("token is not yet valid")
	ErrWrongKeyID         = errors.New("wrong key ID")
	ErrTokenTooLarge      = errors.New("token exceeds max size")
)

const maxLicenseTokenBytes = 16 * 1024

// DecodeUnverified parses the token claims without verifying the signature.
func DecodeUnverified(tokenStr string) (*LicenseClaims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidTokenFormat
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	var claims LicenseClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, err
	}
	return &claims, nil
}

// VerifyJWT verifies the Ed25519 signature of the JWT token string.
func VerifyJWT(tokenStr string, pubKey ed25519.PublicKey) (*LicenseClaims, error) {
	if len(tokenStr) > maxLicenseTokenBytes {
		return nil, ErrTokenTooLarge
	}
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidTokenFormat
	}

	signingInput := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, ErrInvalidSignature
	}

	if !ed25519.Verify(pubKey, []byte(signingInput), sig) {
		return nil, ErrInvalidSignature
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidTokenFormat
	}

	var claims LicenseClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, err
	}

	return &claims, nil
}

// ParsePublicKey parses a 32-byte Ed25519 public key from hex or raw bytes.
func ParsePublicKey(keyBytes []byte) (ed25519.PublicKey, error) {
	if len(keyBytes) == 32 {
		return ed25519.PublicKey(keyBytes), nil
	}
	// Fallback/support hex-encoded public key if it's 64 characters
	keyStr := strings.TrimSpace(string(keyBytes))
	if len(keyStr) == 64 {
		var raw [32]byte
		for i := 0; i < 32; i++ {
			var b byte
			_, err := sscanf(keyStr[i*2:i*2+2], "%x", &b)
			if err != nil {
				return nil, errors.New("invalid public key hex")
			}
			raw[i] = b
		}
		return ed25519.PublicKey(raw[:]), nil
	}
	// Try base64 decoding
	decoded, err := base64.StdEncoding.DecodeString(keyStr)
	if err == nil && len(decoded) == 32 {
		return ed25519.PublicKey(decoded), nil
	}
	decodedRaw, err := base64.RawURLEncoding.DecodeString(keyStr)
	if err == nil && len(decodedRaw) == 32 {
		return ed25519.PublicKey(decodedRaw), nil
	}
	return nil, errors.New("invalid public key length")
}

func sscanf(s string, format string, a ...any) (int, error) {
	var val byte
	if len(s) != 2 {
		return 0, errors.New("bad length")
	}
	for i := 0; i < 2; i++ {
		c := s[i]
		var digit byte
		if c >= '0' && c <= '9' {
			digit = c - '0'
		} else if c >= 'a' && c <= 'f' {
			digit = c - 'a' + 10
		} else if c >= 'A' && c <= 'F' {
			digit = c - 'A' + 10
		} else {
			return 0, errors.New("bad char")
		}
		val = val*16 + digit
	}
	*a[0].(*byte) = val
	return 1, nil
}

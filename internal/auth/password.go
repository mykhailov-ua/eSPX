package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/crypto/argon2"
)

// ErrAuthenticationFailed, ErrInvalidPassword, and ErrInsecureHashParameters keep credential failures generic while still signaling policy violations.
var (
	ErrAuthenticationFailed   = errors.New("authentication failed")
	ErrInvalidPassword        = errors.New("password cannot be empty or exceeds maximum length")
	ErrInsecureHashParameters = errors.New("hash parameters are below minimum security thresholds")
)

// MaxPasswordLength caps password size to bound Argon2 memory use and reject denial-of-service payloads.
const MaxPasswordLength = 72

// ValidatePassword rejects weak secrets before Argon2 work or storage amplifies attacker cost.
func ValidatePassword(password string) error {
	if len(password) < 8 {
		return errors.Join(ErrInvalidPassword, errors.New("password must be at least 8 characters"))
	}
	if len(password) > MaxPasswordLength {
		return errors.Join(ErrInvalidPassword, errors.New("password exceeds maximum length"))
	}

	var hasUpper, hasLower, hasDigit, hasSpecial bool
	for i := 0; i < len(password); i++ {
		c := password[i]
		switch {
		case c >= 'A' && c <= 'Z':
			hasUpper = true
		case c >= 'a' && c <= 'z':
			hasLower = true
		case c >= '0' && c <= '9':
			hasDigit = true
		default:
			hasSpecial = true
		}
	}

	if !hasUpper || !hasLower || !hasDigit || !hasSpecial {
		return errors.Join(ErrInvalidPassword, errors.New("password must contain at least one uppercase letter, one lowercase letter, one digit, and one special character"))
	}
	return nil
}

// params holds decoded Argon2 settings from a stored hash so verification can replay the original work factor.
type params struct {
	memory      uint32
	iterations  uint32
	parallelism uint8
	saltLength  uint32
	keyLength   uint32
}

// Argon2 parameter ceilings and floors bound verification cost and reject weak stored hashes.
const (
	maxMemory      uint32 = 256 * 1024
	maxIterations  uint32 = 10
	maxParallelism uint8  = 32
	minSaltLength  uint32 = 16
	minHashLength  uint32 = 32
	minMemory      uint32 = 32768
	minIterations  uint32 = 2
	minParallelism uint8  = 2
)

// PasswordHasher centralizes Argon2id hashing with a precomputed dummy hash to equalize login timing for unknown users.
type PasswordHasher struct {
	memory      uint32
	iterations  uint32
	saltLength  uint32
	keyLength   uint32
	parallelism uint8
	dummyHash   string
}

// NewPasswordHasher precomputes a dummy hash so unknown emails still pay full verification cost.
func NewPasswordHasher(memory, iterations uint32, parallelism uint8) (*PasswordHasher, error) {
	h := &PasswordHasher{
		memory:      memory,
		iterations:  iterations,
		parallelism: parallelism,
		saltLength:  16,
		keyLength:   32,
	}
	var err error
	h.dummyHash, err = h.HashPassword("dummy-password-timing-attack")
	if err != nil {
		return nil, errors.Join(errors.New("failed to pre-compute dummy hash"), err)
	}
	return h, nil
}

// GetDummyHash hides whether an email exists during failed login attempts.
func (h *PasswordHasher) GetDummyHash() string {
	return h.dummyHash
}

// GetParallelism feeds service-wide crypto limits derived from Argon2 thread count.
func (h *PasswordHasher) GetParallelism() uint8 {
	return h.parallelism
}

// HashPassword embeds Argon2 parameters in the string so verification can honor legacy work factors.
func (h *PasswordHasher) HashPassword(password string) (string, error) {
	if password == "" || len(password) > MaxPasswordLength {
		return "", ErrInvalidPassword
	}

	salt := make([]byte, h.saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", errors.Join(errors.New("failed to generate salt"), err)
	}

	hash := argon2.IDKey(
		[]byte(password),
		salt,
		h.iterations,
		h.memory,
		h.parallelism,
		h.keyLength,
	)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	var sb strings.Builder
	sb.WriteString("$argon2id$v=")
	sb.WriteString(strconv.Itoa(argon2.Version))
	sb.WriteString("$m=")
	sb.WriteString(strconv.FormatUint(uint64(h.memory), 10))
	sb.WriteString(",t=")
	sb.WriteString(strconv.FormatUint(uint64(h.iterations), 10))
	sb.WriteString(",p=")
	sb.WriteString(strconv.FormatUint(uint64(h.parallelism), 10))
	sb.WriteByte('$')
	sb.WriteString(b64Salt)
	sb.WriteByte('$')
	sb.WriteString(b64Hash)

	return sb.String(), nil
}

// unsafeStringToBytes avoids per-verify allocations on the login hot path.
func unsafeStringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// VerifyPassword flags weak stored parameters so login can trigger transparent rehashing.
func VerifyPassword(password, encodedHash string) (bool, error) {
	if password == "" || len(password) > MaxPasswordLength {
		return false, ErrAuthenticationFailed
	}

	const prefix = "$argon2id$v="
	if !strings.HasPrefix(encodedHash, prefix) {
		return false, ErrAuthenticationFailed
	}

	idx1 := len(prefix)
	idx2 := strings.IndexByte(encodedHash[idx1:], '$')
	if idx2 == -1 {
		return false, ErrAuthenticationFailed
	}
	idx2 += idx1

	versionStr := encodedHash[idx1:idx2]
	version, err := strconv.Atoi(versionStr)
	if err != nil || version != argon2.Version {
		return false, ErrAuthenticationFailed
	}

	idx3 := idx2 + 1
	idx4 := strings.IndexByte(encodedHash[idx3:], '$')
	if idx4 == -1 {
		return false, ErrAuthenticationFailed
	}
	idx4 += idx3

	paramsStr := encodedHash[idx3:idx4]
	p := params{}

	sIdx := 0
	for sIdx < len(paramsStr) {
		eIdx := strings.IndexByte(paramsStr[sIdx:], ',')
		var part string
		if eIdx == -1 {
			part = paramsStr[sIdx:]
			sIdx = len(paramsStr)
		} else {
			part = paramsStr[sIdx : sIdx+eIdx]
			sIdx += eIdx + 1
		}

		if strings.HasPrefix(part, "m=") {
			m, err := strconv.ParseUint(part[2:], 10, 32)
			if err != nil || m > uint64(maxMemory) {
				return false, ErrAuthenticationFailed
			}
			p.memory = uint32(m)
		} else if strings.HasPrefix(part, "t=") {
			t, err := strconv.ParseUint(part[2:], 10, 32)
			if err != nil || t > uint64(maxIterations) {
				return false, ErrAuthenticationFailed
			}
			p.iterations = uint32(t)
		} else if strings.HasPrefix(part, "p=") {
			pr, err := strconv.ParseUint(part[2:], 10, 8)
			if err != nil || pr > uint64(maxParallelism) {
				return false, ErrAuthenticationFailed
			}
			p.parallelism = uint8(pr)
		} else {
			return false, ErrAuthenticationFailed
		}
	}

	idx5 := idx4 + 1
	idx6 := strings.IndexByte(encodedHash[idx5:], '$')
	if idx6 == -1 {
		return false, ErrAuthenticationFailed
	}
	idx6 += idx5

	b64Salt := encodedHash[idx5:idx6]
	b64Hash := encodedHash[idx6+1:]

	saltLen := base64.RawStdEncoding.DecodedLen(len(b64Salt))
	if uint32(saltLen) < minSaltLength {
		return false, ErrAuthenticationFailed
	}

	hashLen := base64.RawStdEncoding.DecodedLen(len(b64Hash))
	if uint32(hashLen) < minHashLength {
		return false, ErrAuthenticationFailed
	}

	var saltBuf [64]byte
	var hashBuf [128]byte

	if saltLen > len(saltBuf) || hashLen > len(hashBuf) {
		return false, ErrAuthenticationFailed
	}

	nSalt, err := base64.RawStdEncoding.Decode(saltBuf[:], unsafeStringToBytes(b64Salt))
	if err != nil || uint32(nSalt) < minSaltLength {
		return false, ErrAuthenticationFailed
	}
	salt := saltBuf[:nSalt]
	p.saltLength = uint32(nSalt)

	nHash, err := base64.RawStdEncoding.Decode(hashBuf[:], unsafeStringToBytes(b64Hash))
	if err != nil || uint32(nHash) < minHashLength {
		return false, ErrAuthenticationFailed
	}
	hash := hashBuf[:nHash]
	p.keyLength = uint32(nHash)

	var passwordBuf [128]byte
	copy(passwordBuf[:], password)
	passwordBytes := passwordBuf[:len(password)]

	comparisonHash := argon2.IDKey(passwordBytes, salt, p.iterations, p.memory, p.parallelism, p.keyLength)

	if subtle.ConstantTimeCompare(hash, comparisonHash) == 1 {
		if p.memory < minMemory || p.iterations < minIterations || p.parallelism < minParallelism {
			return true, ErrInsecureHashParameters
		}
		return true, nil
	}

	return false, ErrAuthenticationFailed
}

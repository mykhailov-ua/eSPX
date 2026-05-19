package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

var (
	ErrAuthenticationFailed   = errors.New("authentication failed")
	ErrInvalidPassword        = errors.New("password cannot be empty or exceeds maximum length")
	ErrInsecureHashParameters = errors.New("hash parameters are below minimum security thresholds")
)

const MaxPasswordLength = 72

type params struct {
	memory      uint32
	iterations  uint32
	parallelism uint8
	saltLength  uint32
	keyLength   uint32
}

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

// PasswordHasher encapsulates Argon2id configuration state and maintains a pre-computed dummy hash.
// The dummy hash neutralizes timing side-channels during authentication of non-existent users.
type PasswordHasher struct {
	memory      uint32
	iterations  uint32
	saltLength  uint32
	keyLength   uint32
	parallelism uint8
	dummyHash   string
}

func NewPasswordHasher(memory, iterations uint32, parallelism uint8) *PasswordHasher {
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
		panic(fmt.Sprintf("failed to pre-compute dummy hash: %v", err))
	}
	return h
}

func (h *PasswordHasher) GetDummyHash() string {
	return h.dummyHash
}

func (h *PasswordHasher) HashPassword(password string) (string, error) {
	if password == "" || len(password) > MaxPasswordLength {
		return "", ErrInvalidPassword
	}

	salt := make([]byte, h.saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("failed to generate salt: %w", err)
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

	encodedHash := fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		h.memory,
		h.iterations,
		h.parallelism,
		b64Salt,
		b64Hash,
	)

	return encodedHash, nil
}

func VerifyPassword(password, encodedHash string) (bool, error) {
	if password == "" || len(password) > MaxPasswordLength {
		return false, ErrAuthenticationFailed
	}

	vals := strings.SplitN(encodedHash, "$", 6)
	if len(vals) != 6 {
		return false, ErrAuthenticationFailed
	}

	if vals[1] != "argon2id" {
		return false, ErrAuthenticationFailed
	}

	if !strings.HasPrefix(vals[2], "v=") {
		return false, ErrAuthenticationFailed
	}
	version, err := strconv.Atoi(vals[2][2:])
	if err != nil || version != argon2.Version {
		return false, ErrAuthenticationFailed
	}

	p := params{}
	parts := strings.Split(vals[3], ",")
	if len(parts) != 3 {
		return false, ErrAuthenticationFailed
	}

	for _, part := range parts {
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

	salt, err := base64.RawStdEncoding.DecodeString(vals[4])
	if err != nil || uint32(len(salt)) < minSaltLength {
		return false, ErrAuthenticationFailed
	}
	p.saltLength = uint32(len(salt))

	hash, err := base64.RawStdEncoding.DecodeString(vals[5])
	if err != nil || uint32(len(hash)) < minHashLength {
		return false, ErrAuthenticationFailed
	}
	p.keyLength = uint32(len(hash))

	comparisonHash := argon2.IDKey([]byte(password), salt, p.iterations, p.memory, p.parallelism, p.keyLength)

	// ConstantTimeCompare prevents timing attacks when validating the derived hash against the stored hash.
	if subtle.ConstantTimeCompare(hash, comparisonHash) == 1 {
		if p.memory < minMemory || p.iterations < minIterations || p.parallelism < minParallelism {
			return true, ErrInsecureHashParameters
		}
		return true, nil
	}

	return false, ErrAuthenticationFailed
}

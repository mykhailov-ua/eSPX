package auth

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/o1egl/paseto/v2"
	"golang.org/x/crypto/chacha20poly1305"
)

// PasetoMaker is the production Maker that encrypts access tokens with symmetric PASETO v2.
type PasetoMaker struct {
	paseto       *paseto.V2
	symmetricKey []byte
}

// NewPasetoMaker rejects mis-sized keys at startup instead of failing every token operation at runtime.
func NewPasetoMaker(symmetricKey string) (Maker, error) {
	if len(symmetricKey) != chacha20poly1305.KeySize {
		return nil, errors.New("invalid key size")
	}

	maker := &PasetoMaker{
		paseto:       paseto.NewV2(),
		symmetricKey: []byte(symmetricKey),
	}
	return maker, nil
}

// CreateToken binds access tokens to session and tenant context for revocation and RBAC downstream.
func (maker *PasetoMaker) CreateToken(userID uuid.UUID, sessionID uuid.UUID, role string, customerID uuid.UUID, duration time.Duration) (string, error) {
	payload, err := NewPayload(userID, sessionID, role, customerID, duration)
	if err != nil {
		return "", err
	}

	return maker.paseto.Encrypt(maker.symmetricKey, payload, nil)
}

// VerifyToken decrypts locally so callers never need direct access to the signing key material.
func (maker *PasetoMaker) VerifyToken(token string) (*Payload, error) {
	payload := &Payload{}

	err := maker.paseto.Decrypt(token, maker.symmetricKey, payload, nil)
	if err != nil {
		return nil, ErrInvalidToken
	}

	err = payload.Valid()
	if err != nil {
		return nil, err
	}

	return payload, nil
}

package auth

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidToken and ErrExpiredToken give callers stable auth failure reasons without exposing token internals.
var (
	ErrInvalidToken = errors.New("token is invalid")
	ErrExpiredToken = errors.New("token has expired")
)

// Payload holds the identity claims carried inside an access token for authorization and revocation checks.
type Payload struct {
	ID         uuid.UUID `json:"id"`
	UserID     uuid.UUID `json:"user_id"`
	SessionID  uuid.UUID `json:"session_id"`
	Role       string    `json:"role"`
	CustomerID uuid.UUID `json:"customer_id"`
	IssuedAt   time.Time `json:"issued_at"`
	ExpiredAt  time.Time `json:"expired_at"`
}

// NewPayload assigns a unique token id so per-token revocation remains possible.
func NewPayload(userID uuid.UUID, sessionID uuid.UUID, role string, customerID uuid.UUID, duration time.Duration) (*Payload, error) {
	tokenID, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	payload := &Payload{
		ID:         tokenID,
		UserID:     userID,
		SessionID:  sessionID,
		Role:       role,
		CustomerID: customerID,
		IssuedAt:   time.Now(),
		ExpiredAt:  time.Now().Add(duration),
	}
	return payload, nil
}

// Valid rejects clock-skewed or expired tokens before authorization logic trusts them.
func (payload *Payload) Valid() error {
	now := time.Now()
	if now.After(payload.ExpiredAt) {
		return ErrExpiredToken
	}
	if payload.IssuedAt.After(now.Add(5 * time.Second)) {
		return ErrInvalidToken
	}
	return nil
}

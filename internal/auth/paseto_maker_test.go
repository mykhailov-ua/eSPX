package auth

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestPasetoMaker verifies that issued PASETO tokens round-trip with expected identity claims.
func TestPasetoMaker(t *testing.T) {
	maker, err := NewPasetoMaker("12345678901234567890123456789012")
	require.NoError(t, err)

	userID := uuid.New()
	role := "admin"
	customerID := uuid.New()
	duration := time.Minute

	issuedAt := time.Now()
	expiredAt := issuedAt.Add(duration)

	sessionID, _ := uuid.NewRandom()
	token, err := maker.CreateToken(userID, sessionID, role, customerID, duration)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	payload, err := maker.VerifyToken(token)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	require.NotZero(t, payload.ID)
	require.Equal(t, userID, payload.UserID)
	require.Equal(t, role, payload.Role)
	require.Equal(t, customerID, payload.CustomerID)
	require.WithinDuration(t, issuedAt, payload.IssuedAt, time.Second)
	require.WithinDuration(t, expiredAt, payload.ExpiredAt, time.Second)
}

// TestExpiredPasetoToken rejects access tokens that are already past their expiry window.
func TestExpiredPasetoToken(t *testing.T) {
	maker, err := NewPasetoMaker("12345678901234567890123456789012")
	require.NoError(t, err)

	token, err := maker.CreateToken(uuid.New(), uuid.New(), "admin", uuid.New(), -time.Minute)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	payload, err := maker.VerifyToken(token)
	require.Error(t, err)
	require.EqualError(t, err, ErrExpiredToken.Error())
	require.Nil(t, payload)
}

// TestInvalidPasetoToken rejects tokens encrypted with a different symmetric key.
func TestInvalidPasetoToken(t *testing.T) {
	maker1, _ := NewPasetoMaker("12345678901234567890123456789012")
	maker2, _ := NewPasetoMaker("00000000000000000000000000000000")

	token, _ := maker1.CreateToken(uuid.New(), uuid.New(), "admin", uuid.New(), time.Minute)

	payload, err := maker2.VerifyToken(token)
	require.Error(t, err)
	require.EqualError(t, err, ErrInvalidToken.Error())
	require.Nil(t, payload)
}

// TestNewPasetoMaker_InvalidKey rejects misconfigured encryption keys at startup.
func TestNewPasetoMaker_InvalidKey(t *testing.T) {
	_, err := NewPasetoMaker("short")
	require.Error(t, err)
}

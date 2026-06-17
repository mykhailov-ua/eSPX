package auth

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_RefreshTokenReuseBlocked proves replay of a rotated refresh token is denied.
func TestIntegration_RefreshTokenReuseBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "refresh-reuse@example.com"
	password := "SuperSecure123!"

	userID, _, refreshA := infra.registerAndLogin(t, svc, email, password)

	_, refreshB, err := svc.RefreshToken(ctx, refreshA, time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, refreshB)
	assert.Equal(t, 1, countActiveSessions(t, infra.Pool, userID), "exactly one active session after rotation")

	// Attacker replays stolen refresh after victim rotated (not a legitimate idempotent retry).
	require.NoError(t, infra.Redis.Del(ctx, "idempotency:refresh:"+refreshA).Err())

	_, _, err = svc.RefreshToken(ctx, refreshA, time.Hour)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSessionBlocked)

	_, refreshC, err := svc.RefreshToken(ctx, refreshB, time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, refreshC)
	assert.Equal(t, 1, countActiveSessions(t, infra.Pool, userID))
}

// TestIntegration_BlockUserRevokesInFlightAccessTokens proves BlockUser denies VerifyToken for in-flight access tokens.
func TestIntegration_BlockUserRevokesInFlightAccessTokens(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "block-user@example.com"
	password := "SuperSecure123!"

	userID, accessToken, _ := infra.registerAndLogin(t, svc, email, password)

	_, err := svc.VerifyToken(ctx, accessToken)
	require.NoError(t, err, "baseline verify before block")

	require.NoError(t, svc.BlockUser(ctx, email))

	revokedKey := "revoked:user:" + userID.String()
	exists, err := infra.Redis.Exists(ctx, revokedKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), exists, "revoked:user marker must be set in Redis")

	for range 5 {
		_, err := svc.VerifyToken(ctx, accessToken)
		require.ErrorIs(t, err, ErrSessionBlocked)
	}
}

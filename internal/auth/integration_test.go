package auth

import (
	"context"
	"testing"
	"time"

	"espx/internal/auth/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuthService_Integration exercises registration, login, password reuse, and email verification against real stores.
func TestAuthService_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping testcontainers-based integration test in short mode")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	service := infra.newService(t)
	store := infra.Store

	ctx := context.Background()
	email := "compliance-officer@company.internal"
	initPassword := "SuperSecure123!"

	userID, err := service.Register(ctx, RegisterDTO{
		Email:    email,
		Password: initPassword,
	})
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, userID)

	history, err := store.GetPasswordHistory(ctx, db.GetPasswordHistoryParams{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
		Limit:  10,
	})
	require.NoError(t, err)
	require.Len(t, history, 1, "Initial password must be in history to prevent instant cyclic reuse")

	_, err = service.Register(ctx, RegisterDTO{
		Email:    email,
		Password: "AnotherPassword456!",
	})
	assert.ErrorIs(t, err, ErrUserAlreadyExists, "Registration of duplicates must fail neutrally")

	loginResp, err := service.Login(ctx, email, initPassword, "Mozilla/Firefox", "192.168.1.100", time.Hour)
	require.NoError(t, err)
	assert.NotEmpty(t, loginResp.AccessToken)

	_, err = infra.Pool.Exec(ctx, "UPDATE users SET email_verified = FALSE WHERE email = $1", email)
	require.NoError(t, err)

	_, err = service.Login(ctx, email, initPassword, "Mozilla/Firefox", "192.168.1.100", time.Hour)
	assert.ErrorIs(t, err, ErrEmailNotVerified)

	err = service.ChangePassword(ctx, userID, initPassword, initPassword, "192.168.1.100", "Mozilla/Firefox")
	assert.ErrorIs(t, err, ErrPasswordReuse, "Password reuse check must reject matching historical hashes")

	newPassword1 := "RotatedPassword456!"
	err = service.ChangePassword(ctx, userID, initPassword, newPassword1, "192.168.1.100", "Mozilla/Firefox")
	require.NoError(t, err, "Valid, non-reused password change should succeed")

	history, err = store.GetPasswordHistory(ctx, db.GetPasswordHistoryParams{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
		Limit:  10,
	})
	require.NoError(t, err)
	assert.Len(t, history, 2, "Password history should track new password hash")

	newPassword2 := "ThirdExcellentPass789!"
	err = service.ChangePassword(ctx, userID, newPassword1, newPassword2, "192.168.1.100", "Mozilla/Firefox")
	require.NoError(t, err)

	history, err = store.GetPasswordHistory(ctx, db.GetPasswordHistoryParams{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
		Limit:  10,
	})
	require.NoError(t, err)
	assert.Len(t, history, 3)

	err = service.ChangePassword(ctx, userID, newPassword2, initPassword, "192.168.1.100", "Mozilla/Firefox")
	assert.ErrorIs(t, err, ErrPasswordReuse, "SuperSecure123! is still in the last 3 history and must be blocked")

	token, err := service.RequestEmailVerification(ctx, userID)
	require.NoError(t, err)
	assert.NotEmpty(t, token)

	usr, err := store.GetUserByID(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	require.NoError(t, err)
	assert.False(t, usr.EmailVerified)

	confirmedUID, err := service.ConfirmEmailVerification(ctx, token)
	require.NoError(t, err)
	assert.Equal(t, userID, confirmedUID)

	usr, err = store.GetUserByID(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	require.NoError(t, err)
	assert.True(t, usr.EmailVerified, "Confirming email must persist the verified flag to Postgres")

	loginResp, err = service.Login(ctx, email, newPassword2, "Mozilla/Firefox", "192.168.1.100", time.Hour)
	require.NoError(t, err)
	assert.NotEmpty(t, loginResp.AccessToken)

	_, err = service.ConfirmEmailVerification(ctx, token)
	assert.ErrorIs(t, err, ErrInvalidToken, "Replaying a verification token must be rejected because it was deleted on first use")
}

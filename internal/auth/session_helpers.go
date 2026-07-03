package auth

import (
	"context"
	"strings"
	"time"

	"espx/internal/auth/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

const refreshSessionTTL = 7 * 24 * time.Hour

// parseRefreshIdempotency splits a cached refresh rotation payload into access and refresh tokens.
func parseRefreshIdempotency(cached string) (accessToken, refreshToken string, ok bool) {
	parts := strings.Split(cached, " ")
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// createRefreshSession persists a new refresh-token session row bound to the access token session id.
func createRefreshSession(ctx context.Context, q db.Querier, userID pgtype.UUID, sessionID uuid.UUID, refreshToken, userAgent, clientIP string) error {
	_, err := q.CreateSession(ctx, db.CreateSessionParams{
		ID:           toPgUUID(sessionID),
		UserID:       userID,
		RefreshToken: refreshToken,
		UserAgent:    userAgent,
		ClientIp:     clientIP,
		IsBlocked:    false,
		ExpiresAt:    pgtype.Timestamptz{Time: time.Now().Add(refreshSessionTTL), Valid: true},
	})
	return err
}

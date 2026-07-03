package auth

import (
	"time"

	"github.com/google/uuid"
)

// Maker isolates token format details so login, middleware, and refresh flows stay stable when the signing backend changes.
type Maker interface {
	// CreateToken issues a short-lived access token bound to session and tenant for downstream RBAC.
	CreateToken(userID uuid.UUID, sessionID uuid.UUID, role string, customerID uuid.UUID, duration time.Duration) (string, error)
	// VerifyToken validates access tokens without exposing signing key material to every service.
	VerifyToken(token string) (*Payload, error)
}

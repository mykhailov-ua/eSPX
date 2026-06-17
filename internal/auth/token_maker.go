package auth

import (
	"time"

	"github.com/google/uuid"
)

// Maker isolates token format details so login, middleware, and refresh flows stay stable when the signing backend changes.
type Maker interface {
	CreateToken(userID uuid.UUID, sessionID uuid.UUID, role string, customerID uuid.UUID, duration time.Duration) (string, error)
	VerifyToken(token string) (*Payload, error)
}

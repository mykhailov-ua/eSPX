package auth

import (
	"time"

	"github.com/google/uuid"
)

type Maker interface {
	CreateToken(userID uuid.UUID, role string, customerID uuid.UUID, duration time.Duration) (string, error)
	VerifyToken(token string) (*Payload, error)
}

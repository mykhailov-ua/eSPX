package auth

import (
	"espx/internal/auth/db"
	"espx/internal/auth/pb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func uuidFromPg(u pgtype.UUID) uuid.UUID {
	return uuid.UUID(u.Bytes)
}

// userToPB omits password hashes and internal flags from outward-facing responses.
func userToPB(user db.User) *pb.User {
	return &pb.User{
		Id:         uuidFromPg(user.ID).String(),
		Email:      user.Email,
		Role:       user.Role,
		CustomerId: uuidFromPg(user.CustomerID).String(),
		CreatedAt:  timestamppb.New(user.CreatedAt.Time),
	}
}

// apiKeyRowToPB maps a stored API key row to the gRPC view without secret material.
func apiKeyRowToPB(row db.ListUserAPIKeysRow) *pb.APIKey {
	key := &pb.APIKey{
		Id:        uuidFromPg(row.ID).String(),
		Name:      row.Name,
		CreatedAt: timestamppb.New(row.CreatedAt.Time),
	}
	if row.ExpiresAt.Valid {
		key.ExpiresAt = timestamppb.New(row.ExpiresAt.Time)
	}
	return key
}

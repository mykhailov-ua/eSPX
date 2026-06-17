package auth

import (
	"context"
	"testing"

	"espx/internal/auth/pb"
	"espx/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestHandler_RegisterRequiresAdminKey proves registration stays closed without the admin secret.
func TestHandler_RegisterRequiresAdminKey(t *testing.T) {
	handler := NewHandler(nil, &config.Config{AdminAPIKey: "secret-admin-key"})

	_, err := handler.Register(context.Background(), &pb.RegisterRequest{
		Email:    "user@example.com",
		Password: "Password123!",
		Role:     "U",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
}

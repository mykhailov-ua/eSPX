package management

import (
	"context"
	"errors"

	authpb "espx/internal/auth/pb"

	"google.golang.org/grpc/metadata"
)

var errAuthUnavailable = errors.New("auth service not configured")

func bearerOutgoingContext(ctx context.Context, bearerToken string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearerToken)
}

// AuthClient wraps auth gRPC calls from the management gateway.
type AuthClient struct {
	client authpb.AuthServiceClient
}

// NewAuthClient binds an existing auth gRPC client when management shares the auth connection.
func NewAuthClient(client authpb.AuthServiceClient) *AuthClient {
	if client == nil {
		return nil
	}
	return &AuthClient{client: client}
}

// VerifyAPIKey resolves a self-serve API secret to the owning user principal.
func (c *AuthClient) VerifyAPIKey(ctx context.Context, apiKey string) (*authpb.VerifyAPIKeyResponse, error) {
	if c == nil || c.client == nil {
		return nil, errAuthUnavailable
	}
	return c.client.VerifyAPIKey(ctx, &authpb.VerifyAPIKeyRequest{ApiKey: apiKey})
}

// CreateAPIKey mints a long-lived credential for the bearer-authenticated session user.
func (c *AuthClient) CreateAPIKey(ctx context.Context, bearerToken, name string) (*authpb.CreateAPIKeyResponse, error) {
	if c == nil || c.client == nil {
		return nil, errAuthUnavailable
	}
	grpcCtx := bearerOutgoingContext(ctx, bearerToken)
	return c.client.CreateAPIKey(grpcCtx, &authpb.CreateAPIKeyRequest{Name: name})
}

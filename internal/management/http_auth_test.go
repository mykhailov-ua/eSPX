package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"espx/internal/auth"
	"espx/internal/auth/pb"
	"espx/internal/config"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockAuthClient struct {
	pb.AuthServiceClient
	loginFunc   func(ctx context.Context, in *pb.LoginRequest) (*pb.LoginResponse, error)
	revokeFunc  func(ctx context.Context, in *pb.RevokeTokenRequest) (*pb.RevokeTokenResponse, error)
	refreshFunc func(ctx context.Context, in *pb.RefreshTokenRequest) (*pb.RefreshTokenResponse, error)
}

func (m *mockAuthClient) Login(ctx context.Context, in *pb.LoginRequest, opts ...grpc.CallOption) (*pb.LoginResponse, error) {
	if m.loginFunc != nil {
		return m.loginFunc(ctx, in)
	}
	return nil, errors.New("unexpected call to Login")
}

func (m *mockAuthClient) RevokeToken(ctx context.Context, in *pb.RevokeTokenRequest, opts ...grpc.CallOption) (*pb.RevokeTokenResponse, error) {
	if m.revokeFunc != nil {
		return m.revokeFunc(ctx, in)
	}
	return nil, errors.New("unexpected call to RevokeToken")
}

func (m *mockAuthClient) RefreshToken(ctx context.Context, in *pb.RefreshTokenRequest, opts ...grpc.CallOption) (*pb.RefreshTokenResponse, error) {
	if m.refreshFunc != nil {
		return m.refreshFunc(ctx, in)
	}
	return nil, errors.New("unexpected call to RefreshToken")
}

// TestAuthHandler_Login guards login sets secure cookies, CSRF token, and role permissions on success.
func TestAuthHandler_Login(t *testing.T) {
	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	mockClient := &mockAuthClient{
		loginFunc: func(ctx context.Context, in *pb.LoginRequest) (*pb.LoginResponse, error) {
			if in.Email == "test@example.com" && in.Password == "correctPass" {
				return &pb.LoginResponse{
					AccessToken:  "access-token-jwt",
					RefreshToken: "refresh-token-uuid",
					User: &pb.User{
						Id:         "user-123",
						Email:      "test@example.com",
						Role:       "admin",
						CustomerId: "customer-456",
						CreatedAt:  timestamppb.Now(),
					},
				}, nil
			}
			return nil, errors.New("invalid credentials")
		},
	}

	h := NewAuthHandler(mockClient, tokenMaker, nil, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	t.Run("ValidLogin", func(t *testing.T) {
		reqBody := map[string]string{"email": "test@example.com", "password": "correctPass"}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", "/api/v1/auth/login", bytes.NewBuffer(body))
		resp := httptest.NewRecorder()

		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		cookies := resp.Header().Values("Set-Cookie")
		require.Len(t, cookies, 3)

		var accessSet, refreshSet, csrfSet bool
		for _, c := range cookies {
			if strings.HasPrefix(c, "accessToken=") {
				accessSet = true
				assert.Contains(t, c, "HttpOnly")
				assert.Contains(t, c, "Secure")
				assert.Contains(t, c, "SameSite=Strict")
				assert.Contains(t, c, "Max-Age=3600")
			}
			if strings.HasPrefix(c, "refreshToken=") {
				refreshSet = true
				assert.Contains(t, c, "HttpOnly")
				assert.Contains(t, c, "Secure")
				assert.Contains(t, c, "SameSite=Strict")
				assert.Contains(t, c, "Max-Age=2592000")
			}
			if strings.HasPrefix(c, "csrfToken=") {
				csrfSet = true
				assert.NotContains(t, c, "HttpOnly")
				assert.Contains(t, c, "Secure")
				assert.Contains(t, c, "SameSite=Strict")
				assert.Contains(t, c, "Max-Age=3600")
			}
		}
		assert.True(t, accessSet)
		assert.True(t, refreshSet)
		assert.True(t, csrfSet)
		assert.NotEmpty(t, resp.Header().Get("X-CSRF-Token"))

		var res map[string]UserDTO
		err := json.NewDecoder(resp.Body).Decode(&res)
		require.NoError(t, err)
		assert.Equal(t, "user-123", res["user"].ID)
		assert.Equal(t, RoleAdmin, res["user"].Role)
		assert.Contains(t, res["user"].Permissions, "customers:write")
	})

	t.Run("InvalidCredentials", func(t *testing.T) {
		reqBody := map[string]string{"email": "test@example.com", "password": "wrongPass"}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", "/api/v1/auth/login", bytes.NewBuffer(body))
		resp := httptest.NewRecorder()

		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusUnauthorized, resp.Code)
	})
}

// TestAuthHandler_Logout guards logout revokes refresh token and clears session cookies.
func TestAuthHandler_Logout(t *testing.T) {
	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	tokenMaker, _ := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))

	var revokedToken string
	mockClient := &mockAuthClient{
		revokeFunc: func(ctx context.Context, in *pb.RevokeTokenRequest) (*pb.RevokeTokenResponse, error) {
			revokedToken = in.RefreshToken
			return &pb.RevokeTokenResponse{}, nil
		},
	}

	h := NewAuthHandler(mockClient, tokenMaker, nil, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req, _ := http.NewRequest("POST", "/api/v1/auth/logout", nil)
	req.AddCookie(&http.Cookie{Name: "refreshToken", Value: "token-to-revoke"})
	resp := httptest.NewRecorder()

	mux.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusNoContent, resp.Code)
	assert.Equal(t, "token-to-revoke", revokedToken)

	cookies := resp.Header().Values("Set-Cookie")
	for _, c := range cookies {
		assert.Contains(t, c, "Max-Age=0")
	}
}

// TestAuthHandler_Refresh guards refresh rotates access and refresh cookies on valid token.
func TestAuthHandler_Refresh(t *testing.T) {
	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	tokenMaker, _ := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))

	mockClient := &mockAuthClient{
		refreshFunc: func(ctx context.Context, in *pb.RefreshTokenRequest) (*pb.RefreshTokenResponse, error) {
			if in.RefreshToken == "valid-refresh" {
				return &pb.RefreshTokenResponse{
					AccessToken:  "new-access",
					RefreshToken: "new-refresh",
				}, nil
			}
			return nil, errors.New("invalid refresh")
		},
	}

	h := NewAuthHandler(mockClient, tokenMaker, nil, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	t.Run("ValidRefresh", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/auth/refresh", nil)
		req.AddCookie(&http.Cookie{Name: "refreshToken", Value: "valid-refresh"})
		resp := httptest.NewRecorder()

		mux.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		cookies := resp.Header().Values("Set-Cookie")
		require.Len(t, cookies, 2)
	})
}

// TestAuthHandler_Me guards /me returns identity and permissions from a valid access token.
func TestAuthHandler_Me(t *testing.T) {
	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	tokenMaker, _ := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))

	userID := uuid.New()
	customerID := uuid.New()
	sessionID := uuid.New()
	token, err := tokenMaker.CreateToken(userID, sessionID, "admin", customerID, time.Hour)
	require.NoError(t, err)

	h := NewAuthHandler(nil, tokenMaker, nil, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req, _ := http.NewRequest("GET", "/api/v1/auth/me", nil)
	req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	resp := httptest.NewRecorder()

	mux.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)

	var dto UserDTO
	err = json.NewDecoder(resp.Body).Decode(&dto)
	require.NoError(t, err)
	assert.Equal(t, userID.String(), dto.ID)
	assert.Equal(t, RoleAdmin, dto.Role)
	assert.Equal(t, customerID.String(), dto.CustomerID)
	assert.Contains(t, dto.Permissions, "campaigns:write")
}

// TestAuthHandler_MeRedisOutage guards /me fails closed when session validation Redis is unavailable.
func TestAuthHandler_MeRedisOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()
	_ = rdb.Close()

	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	userID := uuid.New()
	customerID := uuid.New()
	token, err := tokenMaker.CreateToken(userID, uuid.New(), RoleAdmin, customerID, time.Hour)
	require.NoError(t, err)

	h := NewAuthHandler(nil, tokenMaker, rdb, cfg, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req, _ := http.NewRequest("GET", "/api/v1/auth/me", nil)
	req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	resp := httptest.NewRecorder()

	mux.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusInternalServerError, resp.Code)
	assert.Contains(t, resp.Body.String(), "security subsystem unavailable")
}

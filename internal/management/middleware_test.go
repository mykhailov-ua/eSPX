package management

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/auth"
	"espx/internal/config"
	"espx/internal/database"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuthMiddleware_RequireAuth guards RequireAuth accepts API key or token and enforces role checks.
func TestAuthMiddleware_RequireAuth(t *testing.T) {
	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
		AdminAPIKey:       "secret-api-key",
	}
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	m := NewAuthMiddleware(tokenMaker, nil, cfg)

	targetHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, ok := GetUser(r.Context())
		if !ok {
			httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "user not in context")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("role:" + u.Role))
	})

	t.Run("APIKey_Success", func(t *testing.T) {
		handler := m.RequireAuth(RoleAdmin)(targetHandler)

		req, _ := http.NewRequest("GET", "/protected", nil)
		req.Header.Set("X-Admin-API-Key", "secret-api-key")
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "role:"+RoleAdmin, resp.Body.String())
	})

	t.Run("APIKey_StablePrincipal", func(t *testing.T) {
		var captured AuthenticatedUser
		handler := m.RequirePermission(PermSettingsWrite)(func(w http.ResponseWriter, r *http.Request) {
			u, ok := GetUser(r.Context())
			if !ok {
				http.Error(w, "missing user", http.StatusInternalServerError)
				return
			}
			captured = u
			w.WriteHeader(http.StatusOK)
		})

		req, _ := http.NewRequest("GET", "/protected", nil)
		req.Header.Set("X-Admin-API-Key", "secret-api-key")
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, apiKeyPrincipalID("secret-api-key"), captured.UserID)
		assert.Equal(t, "api_key", captured.AuthSource)
	})

	t.Run("ValidToken_AllowedRole", func(t *testing.T) {
		handler := m.RequireAuth(RoleManager, RoleAdmin)(targetHandler)

		token, _ := tokenMaker.CreateToken(uuid.New(), uuid.New(), "manager", uuid.New(), time.Hour)
		req, _ := http.NewRequest("GET", "/protected", nil)
		req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "role:M", resp.Body.String())
	})

	t.Run("ValidToken_ForbiddenRole", func(t *testing.T) {
		handler := m.RequireAuth(RoleAdmin)(targetHandler)

		token, _ := tokenMaker.CreateToken(uuid.New(), uuid.New(), "customer", uuid.New(), time.Hour)
		req, _ := http.NewRequest("GET", "/protected", nil)
		req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusForbidden, resp.Code)
	})

	t.Run("MissingToken", func(t *testing.T) {
		handler := m.RequireAuth(RoleAdmin)(targetHandler)

		req, _ := http.NewRequest("GET", "/protected", nil)
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusUnauthorized, resp.Code)
	})

	t.Run("ExpiredToken", func(t *testing.T) {
		handler := m.RequireAuth(RoleAdmin)(targetHandler)

		token, _ := tokenMaker.CreateToken(uuid.New(), uuid.New(), RoleAdmin, uuid.New(), -time.Hour)
		req, _ := http.NewRequest("GET", "/protected", nil)
		req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusUnauthorized, resp.Code)
	})
}

// TestAuthMiddleware_RedisOutage guards RequireAuth fails closed when session Redis is unavailable.
func TestAuthMiddleware_RedisOutage(t *testing.T) {
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

	m := NewAuthMiddleware(tokenMaker, rdb, cfg)

	targetHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := m.RequireAuth(RoleAdmin)(targetHandler)

	token, _ := tokenMaker.CreateToken(uuid.New(), uuid.New(), RoleAdmin, uuid.New(), time.Hour)
	req, _ := http.NewRequest("GET", "/protected", nil)
	req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusUnauthorized, resp.Code)
}

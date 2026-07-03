package management

import (
	"context"
	"log/slog"
	"net/http"

	"espx/internal/auth"
	"espx/internal/config"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// contextKey is a private type for request-scoped context values to avoid key collisions.
type contextKey string

// UserContextKey is the request context key for the authenticated admin or customer user.
const UserContextKey contextKey = "authenticated_user"

// AuthenticatedUser carries identity and tenancy resolved by auth middleware for downstream handlers.
type AuthenticatedUser struct {
	UserID     uuid.UUID
	Role       string
	CustomerID uuid.UUID
	AuthSource string
}

// IsUser reports whether the caller is a customer-scoped user rather than staff.
func (u AuthenticatedUser) IsUser() bool {
	return u.Role == RoleUser
}

// GetUser reads the authenticated user from context when auth middleware ran successfully.
func GetUser(ctx context.Context) (AuthenticatedUser, bool) {
	u, ok := ctx.Value(UserContextKey).(AuthenticatedUser)
	return u, ok
}

// AuthMiddleware validates tokens or admin API keys and enforces permission-based route access.
type AuthMiddleware struct {
	tokenMaker auth.Maker
	rdb        redis.UniversalClient
	cfg        *config.Config
}

// NewAuthMiddleware constructs middleware that checks JWT cookies, revocations, and optional API keys.
func NewAuthMiddleware(tokenMaker auth.Maker, rdb redis.UniversalClient, cfg *config.Config) *AuthMiddleware {
	return &AuthMiddleware{
		tokenMaker: tokenMaker,
		rdb:        rdb,
		cfg:        cfg,
	}
}

// RequirePermission wraps handlers with authentication and permission checks.
func (m *AuthMiddleware) RequirePermission(permission string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			user, ok := m.authenticate(w, r)
			if !ok {
				return
			}
			if !HasPermission(user.Role, permission) {
				httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: insufficient permissions")
				return
			}
			ctx := context.WithValue(r.Context(), UserContextKey, user)
			next(w, r.WithContext(ctx))
		}
	}
}

// RequireAuth wraps handlers with authentication and role checks for legacy call sites.
func (m *AuthMiddleware) RequireAuth(allowedRoles ...string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			user, ok := m.authenticate(w, r)
			if !ok {
				return
			}
			roleAllowed := false
			for _, allowed := range allowedRoles {
				if user.Role == NormalizeRole(allowed) || user.Role == RoleAdmin {
					roleAllowed = true
					break
				}
			}
			if !roleAllowed {
				httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden: insufficient permissions")
				return
			}
			ctx := context.WithValue(r.Context(), UserContextKey, user)
			next(w, r.WithContext(ctx))
		}
	}
}

// authenticate resolves the caller from a shared API key or session cookie before RBAC checks run.
func (m *AuthMiddleware) authenticate(w http.ResponseWriter, r *http.Request) (AuthenticatedUser, bool) {
	if key := r.Header.Get("X-Admin-API-Key"); key != "" && m.cfg != nil && key == string(m.cfg.AdminAPIKey) {
		return AuthenticatedUser{
			UserID:     apiKeyPrincipalID(key),
			Role:       RoleAdmin,
			CustomerID: uuid.Nil,
			AuthSource: "api_key",
		}, true
	}

	cookie, err := r.Cookie("accessToken")
	if err != nil || cookie.Value == "" {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: missing token")
		return AuthenticatedUser{}, false
	}

	payload, err := m.tokenMaker.VerifyToken(cookie.Value)
	if err != nil {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: invalid token")
		return AuthenticatedUser{}, false
	}

	if m.rdb != nil {
		revoked, errRev := auth.CheckTokenRevocation(r.Context(), m.rdb, payload)
		if errRev != nil {
			slog.Error("redis revocation check failed, blocking request to prevent security bypass", "error", errRev)
			httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: security check failed")
			return AuthenticatedUser{}, false
		}
		if revoked {
			httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: session revoked")
			return AuthenticatedUser{}, false
		}
	}

	return AuthenticatedUser{
		UserID:     payload.UserID,
		Role:       NormalizeRole(payload.Role),
		CustomerID: payload.CustomerID,
		AuthSource: "session",
	}, true
}

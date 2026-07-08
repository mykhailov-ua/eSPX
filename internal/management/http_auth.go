package management

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"espx/internal/auth"
	"espx/internal/auth/pb"
	"espx/internal/config"
	"espx/pkg/cold"
	"espx/pkg/httpresponse"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/metadata"
)

var bufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// putBuffer returns a request body buffer to the pool when it is small enough to reuse safely.
func putBuffer(buf *bytes.Buffer) {
	if buf == nil || buf.Cap() > 64*1024 {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}

// AuthHandler exposes login, logout, refresh, and registration endpoints for the admin UI.
type AuthHandler struct {
	authClient     pb.AuthServiceClient
	tokenMaker     auth.Maker
	rdb            redis.UniversalClient
	cfg            *config.Config
	authMiddleware *AuthMiddleware
}

// NewAuthHandler wires auth HTTP endpoints to the gRPC auth service and token infrastructure.
func NewAuthHandler(authClient pb.AuthServiceClient, tokenMaker auth.Maker, rdb redis.UniversalClient, cfg *config.Config, authMiddleware *AuthMiddleware) *AuthHandler {
	return &AuthHandler{
		authClient:     authClient,
		tokenMaker:     tokenMaker,
		rdb:            rdb,
		cfg:            cfg,
		authMiddleware: authMiddleware,
	}
}

// RegisterRoutes mounts cookie-based auth endpoints on the provided mux.
func (h *AuthHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/auth/login", h.login)
	mux.HandleFunc("POST /api/v1/auth/logout", h.logout)
	mux.HandleFunc("POST /api/v1/auth/refresh", h.refresh)
	mux.HandleFunc("GET /api/v1/auth/me", h.me)
	if h.authMiddleware != nil {
		mux.HandleFunc("POST /api/v1/auth/register", h.authMiddleware.RequirePermission(PermUsersWrite)(h.register))
	} else {
		mux.HandleFunc("POST /api/v1/auth/register", h.register)
	}
}

// setCookie writes hardened session cookies shared by login and logout flows.
func setCookie(w http.ResponseWriter, name, value, path string, maxAge int, httpOnly bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     path,
		MaxAge:   maxAge,
		HttpOnly: httpOnly,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
}

// LoginRequest carries credentials for the cookie-based admin login endpoint.
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// UserDTO exposes identity, role, and permissions to the frontend after authentication.
type UserDTO struct {
	ID          string   `json:"id"`
	Email       string   `json:"email,omitempty"`
	Role        string   `json:"role"`
	CustomerID  string   `json:"customer_id"`
	Permissions []string `json:"permissions,omitempty"`
}

// login authenticates credentials, sets session cookies, and issues a CSRF token for mutating requests.
func (h *AuthHandler) login(w http.ResponseWriter, r *http.Request) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putBuffer(buf)

	if _, err := io.Copy(buf, r.Body); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to read request body")
		return
	}

	req, err := cold.DecodeBody[LoginRequest](buf.Bytes())
	if err != nil || req.Email == "" || req.Password == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid login request")
		return
	}

	resp, err := h.authClient.Login(r.Context(), &pb.LoginRequest{
		Email:         req.Email,
		Password:      req.Password,
		DurationHours: 1,
	})
	if err != nil {
		slog.Warn("login failed", "email", req.Email, "error", err)
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid credentials")
		return
	}

	setCookie(w, "accessToken", resp.AccessToken, "/", 3600, true)
	setCookie(w, "refreshToken", resp.RefreshToken, "/api/v1/auth", 30*24*3600, true)
	csrf, err := GenerateSecureToken(32)
	if err != nil {
		slog.Error("failed to generate secure csrf token due to entropy starvation", "error", err)
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal system failure")
		return
	}
	setCookie(w, "csrfToken", csrf, "/", 3600, false)
	w.Header().Set("X-CSRF-Token", csrf)

	userDTO := UserDTO{
		ID:          resp.User.Id,
		Email:       resp.User.Email,
		Role:        NormalizeRole(resp.User.Role),
		CustomerID:  resp.User.CustomerId,
		Permissions: GetPermissionsForRole(resp.User.Role),
	}

	httpresponse.JSON(w, http.StatusOK, map[string]any{"user": userDTO})
}

// logout revokes refresh tokens, blocklists access tokens, and clears session cookies.
func (h *AuthHandler) logout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("refreshToken")
	if err == nil && cookie.Value != "" {
		if _, errRevoke := h.authClient.RevokeToken(r.Context(), &pb.RevokeTokenRequest{
			RefreshToken: cookie.Value,
		}); errRevoke != nil {
			slog.Warn("failed to revoke token on logout", "error", errRevoke)
		}
	}

	accessCookie, err := r.Cookie("accessToken")
	if err == nil && accessCookie.Value != "" && h.rdb != nil {
		payload, errPayload := h.tokenMaker.VerifyToken(accessCookie.Value)
		if errPayload == nil {
			pipe := h.rdb.Pipeline()
			ttl := time.Until(payload.ExpiredAt)
			pipe.Set(r.Context(), "revoked:token:"+payload.ID.String(), "true", ttl)
			pipe.Set(r.Context(), "revoked:session:"+payload.SessionID.String(), "true", ttl)
			if _, errExec := pipe.Exec(r.Context()); errExec != nil {
				slog.Error("failed to execute pipeline during logout token revocation", "error", errExec)
			}
		}
	}

	setCookie(w, "accessToken", "", "/", -1, true)
	setCookie(w, "refreshToken", "", "/api/v1/auth", -1, true)
	setCookie(w, "csrfToken", "", "/", -1, false)
	httpresponse.JSON(w, http.StatusNoContent, nil)
}

// refresh rotates access and refresh cookies using a valid refresh token.
func (h *AuthHandler) refresh(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("refreshToken")
	if err != nil || cookie.Value == "" {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing refresh token")
		return
	}

	resp, err := h.authClient.RefreshToken(r.Context(), &pb.RefreshTokenRequest{
		RefreshToken: cookie.Value,
	})
	if err != nil {
		slog.Warn("refresh token failed", "error", err)
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
		return
	}

	setCookie(w, "accessToken", resp.AccessToken, "/", 3600, true)
	setCookie(w, "refreshToken", resp.RefreshToken, "/api/v1/auth", 30*24*3600, true)

	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "refreshed"})
}

// me returns the current user profile when the access token is valid and not revoked.
func (h *AuthHandler) me(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("accessToken")
	if err != nil || cookie.Value == "" {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
		return
	}

	payload, err := h.tokenMaker.VerifyToken(cookie.Value)
	if err != nil {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
		return
	}

	if h.rdb != nil {
		revoked, errRev := auth.CheckTokenRevocation(r.Context(), h.rdb, payload)
		if errRev != nil {
			slog.Error("redis revocation check failed on /me, blocking request", "error", errRev)
			httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: security check failed")
			return
		}
		if revoked {
			httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized: token revoked")
			return
		}
	}

	dto := UserDTO{
		ID:          payload.UserID.String(),
		Role:        NormalizeRole(payload.Role),
		CustomerID:  payload.CustomerID.String(),
		Permissions: GetPermissionsForRole(payload.Role),
	}

	httpresponse.JSON(w, http.StatusOK, dto)
}

// RegisterRequest carries admin-provisioned user creation data for the register endpoint.
type RegisterRequest struct {
	Email      string `json:"email"`
	Password   string `json:"password"`
	Role       string `json:"role"`
	CustomerID string `json:"customer_id,omitempty"`
}

// register creates manager or customer users and is restricted to authenticated admins.
func (h *AuthHandler) register(w http.ResponseWriter, r *http.Request) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer putBuffer(buf)

	if _, err := io.Copy(buf, r.Body); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "failed to read request body")
		return
	}

	req, err := cold.DecodeBody[RegisterRequest](buf.Bytes())
	if err != nil || req.Email == "" || req.Password == "" || req.Role == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid register request")
		return
	}

	reqRole := NormalizeRole(req.Role)
	if reqRole != RoleManager && reqRole != RoleUser {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "role must be M or U")
		return
	}
	if reqRole == RoleUser && req.CustomerID == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required for user role")
		return
	}

	resp, err := h.authClient.Register(metadata.AppendToOutgoingContext(r.Context(), "x-admin-api-key", string(h.cfg.AdminAPIKey)), &pb.RegisterRequest{
		Email:      req.Email,
		Password:   req.Password,
		Role:       reqRole,
		CustomerId: req.CustomerID,
	})
	if err != nil {
		slog.Warn("registration failed", "email", req.Email, "error", err)
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "registration failed")
		return
	}

	httpresponse.JSON(w, http.StatusCreated, map[string]any{"user_id": resp.UserId})
}

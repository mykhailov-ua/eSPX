package management

import (
	"context"
	"net/http"
	"os"
	"strings"

	"espx/internal/auth/pb"
	"espx/pkg/httpresponse"
	uiauth "espx/ui/auth"
	uistatic "espx/ui/static"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

// UIHandler serves server-rendered admin pages.
type UIHandler struct {
	authHandler    *AuthHandler
	authMiddleware *AuthMiddleware
	svc            *Service
	grafanaURL     string
}

// NewUIHandler wires HTML admin routes to auth and the management service.
func NewUIHandler(authHandler *AuthHandler, authMiddleware *AuthMiddleware, svc *Service) *UIHandler {
	url := strings.TrimSpace(os.Getenv("GRAFANA_URL"))
	if url == "" {
		url = defaultGrafanaURL
	}
	return &UIHandler{
		authHandler:    authHandler,
		authMiddleware: authMiddleware,
		svc:            svc,
		grafanaURL:     url,
	}
}

// RegisterRoutes mounts admin UI pages and static assets.
func (h *UIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("GET /admin/static/", http.StripPrefix("/admin/static/", http.FileServer(http.FS(uistatic.FS))))

	mux.HandleFunc("GET /admin/", h.root)
	mux.HandleFunc("GET /admin/login", h.loginPage)
	mux.HandleFunc("POST /admin/login", h.loginSubmit)
	mux.HandleFunc("GET /admin/register", h.registerPage)
	mux.HandleFunc("POST /admin/register", h.registerSubmit)
	mux.HandleFunc("POST /admin/logout", h.logout)

	auth := func(next http.HandlerFunc) http.HandlerFunc {
		if h.authMiddleware != nil {
			return h.authMiddleware.RequireAuth(RoleAdmin, RoleManager)(next)
		}
		return next
	}

	mux.HandleFunc("GET /admin/campaigns/manage", auth(h.campaignsList))
	mux.HandleFunc("GET /admin/campaigns/new", auth(h.campaignsNew))
	mux.HandleFunc("POST /admin/campaigns/new", auth(h.campaignsCreate))
	mux.HandleFunc("POST /admin/campaigns/{id}/cancel", auth(h.campaignsCancel))
	mux.HandleFunc("GET /admin/dashboard", auth(h.dashboardRedirect))
}

func (h *UIHandler) root(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/" && r.URL.Path != "/admin" {
		http.NotFound(w, r)
		return
	}
	if cookie, err := r.Cookie("accessToken"); err == nil && cookie.Value != "" {
		http.Redirect(w, r, "/admin/campaigns/manage", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}

func (h *UIHandler) loginPage(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("accessToken"); err == nil && cookie.Value != "" {
		http.Redirect(w, r, "/admin/campaigns/manage", http.StatusSeeOther)
		return
	}

	csrf, err := ensureCSRFCookie(w, r)
	if err != nil {
		slog.Error("failed to set csrf cookie on login page", "error", err)
		httpresponse.HTMXError(w, r, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal error")
		return
	}

	msg := ""
	if r.URL.Query().Get("registered") == "1" {
		msg = "Account created. Sign in to continue."
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiauth.Login(uiauth.LoginPage{CSRF: csrf, Message: msg}).Render(r.Context(), w); err != nil {
		slog.Error("failed to render login page", "error", err)
	}
}

func (h *UIHandler) registerPage(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("accessToken"); err == nil && cookie.Value != "" {
		http.Redirect(w, r, "/admin/campaigns/manage", http.StatusSeeOther)
		return
	}

	csrf, err := ensureCSRFCookie(w, r)
	if err != nil {
		slog.Error("failed to set csrf cookie on register page", "error", err)
		httpresponse.HTMXError(w, r, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal error")
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiauth.Register(uiauth.RegisterPage{CSRF: csrf}).Render(r.Context(), w); err != nil {
		slog.Error("failed to render register page", "error", err)
	}
}

func (h *UIHandler) loginSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderLoginError(w, r, "invalid form")
		return
	}

	email := r.FormValue("email")
	password := r.FormValue("password")
	if email == "" || password == "" {
		h.renderLoginError(w, r, "email and password are required")
		return
	}

	resp, err := h.authHandler.authClient.Login(r.Context(), &pb.LoginRequest{
		Email:         email,
		Password:      password,
		DurationHours: 1,
	})
	if err != nil {
		slog.Warn("ui login failed", "email", email, "error", err)
		h.renderLoginError(w, r, loginErrorMessage(err))
		return
	}

	setCookie(w, r, "accessToken", resp.AccessToken, "/", 3600, true)
	setCookie(w, r, "refreshToken", resp.RefreshToken, "/api/v1/auth", 30*24*3600, true)
	csrf, err := GenerateSecureToken(32)
	if err != nil {
		slog.Error("failed to generate csrf token after login", "error", err)
		httpresponse.HTMXError(w, r, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal error")
		return
	}
	setCookie(w, r, "csrfToken", csrf, "/", 3600, false)
	w.Header().Set("X-CSRF-Token", csrf)

	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", "/admin/campaigns/manage")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/admin/campaigns/manage", http.StatusSeeOther)
}

func (h *UIHandler) registerSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderRegisterError(w, r, "invalid form")
		return
	}

	email := r.FormValue("email")
	password := r.FormValue("password")
	confirm := r.FormValue("password_confirm")
	if email == "" || password == "" {
		h.renderRegisterError(w, r, "email and password are required")
		return
	}
	if password != confirm {
		h.renderRegisterError(w, r, "passwords do not match")
		return
	}

	ctx := metadata.AppendToOutgoingContext(r.Context(), "x-admin-api-key", string(h.authHandler.cfg.AdminAPIKey))
	_, err := h.authHandler.authClient.Register(ctx, &pb.RegisterRequest{
		Email:    email,
		Password: password,
		Role:     RoleManager,
	})
	if err != nil {
		slog.Warn("ui register failed", "email", email, "error", err)
		h.renderRegisterError(w, r, "registration failed: email may already exist or password is too weak")
		return
	}

	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", "/admin/login?registered=1")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/admin/login?registered=1", http.StatusSeeOther)
}

func (h *UIHandler) renderLoginError(w http.ResponseWriter, r *http.Request, message string) {
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = uiauth.LoginErrors(uiauth.FieldErrors{General: message}).Render(r.Context(), w)
		return
	}

	csrf, err := ensureCSRFCookie(w, r)
	if err != nil {
		httpresponse.HTMXError(w, r, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal error")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = uiauth.Login(uiauth.LoginPage{
		CSRF:  csrf,
		Error: message,
	}).Render(r.Context(), w)
}

func (h *UIHandler) renderRegisterError(w http.ResponseWriter, r *http.Request, message string) {
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = uiauth.RegisterErrors(uiauth.FieldErrors{General: message}).Render(r.Context(), w)
		return
	}

	csrf, err := ensureCSRFCookie(w, r)
	if err != nil {
		httpresponse.HTMXError(w, r, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal error")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = uiauth.Register(uiauth.RegisterPage{CSRF: csrf, Error: message}).Render(r.Context(), w)
}

func loginErrorMessage(err error) string {
	msg := err.Error()
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "email_not_verified") {
		return "email not verified"
	}
	if strings.Contains(lower, "locked") || strings.Contains(lower, "ratelimit") {
		return "too many attempts, try again later"
	}
	return "invalid credentials"
}

func (h *UIHandler) logout(w http.ResponseWriter, r *http.Request) {
	setCookie(w, r, "accessToken", "", "/", -1, true)
	setCookie(w, r, "refreshToken", "", "/api/v1/auth", -1, true)
	setCookie(w, r, "csrfToken", "", "/", -1, false)
	w.Header().Set("HX-Redirect", "/admin/login")
	w.WriteHeader(http.StatusOK)
}

func ensureCSRFCookie(w http.ResponseWriter, r *http.Request) (string, error) {
	if cookie, err := r.Cookie("csrfToken"); err == nil && cookie.Value != "" {
		return cookie.Value, nil
	}
	csrf, err := GenerateSecureToken(32)
	if err != nil {
		return "", err
	}
	setCookie(w, r, "csrfToken", csrf, "/", 3600, false)
	return csrf, nil
}

func csrfFromRequest(r *http.Request) string {
	if cookie, err := r.Cookie("csrfToken"); err == nil {
		return cookie.Value
	}
	return ""
}

func userDisplayEmail(_ *http.Request, user AuthenticatedUser) string {
	if user.UserID == uuid.Nil {
		return ""
	}
	if user.AuthSource == "api_key" {
		return "api-key"
	}
	return user.UserID.String()[:8] + "..."
}

// StartDashboardFanout is a no-op kept for wiring compatibility.
func (h *UIHandler) StartDashboardFanout(_ context.Context) {}

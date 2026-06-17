package auth

import (
	"espx/internal/auth/pb"
)

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"espx/internal/auth/db"
	"espx/internal/config"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const adminAPIKeyMetadata = "x-admin-api-key"

// Handler is the gRPC boundary so edge and management services share one auth implementation.
type Handler struct {
	pb.UnimplementedAuthServiceServer
	service *Service
	cfg     *config.Config
}

// NewHandler needs runtime config for trusted-proxy IP rules and token duration defaults.
func NewHandler(service *Service, cfg *config.Config) *Handler {
	return &Handler{
		service: service,
		cfg:     cfg,
	}
}

// extractClientIP resolves the caller IP for audit and lockout keys while ignoring spoofed headers from untrusted peers.
// When X-Real-IP is absent, the rightmost X-Forwarded-For hop is used because nginx appends $remote_addr via $proxy_add_x_forwarded_for.
func (h *Handler) extractClientIP(ctx context.Context) string {
	peerIP := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		host, _, err := net.SplitHostPort(p.Addr.String())
		if err == nil {
			peerIP = host
		} else {
			peerIP = p.Addr.String()
		}
	}

	isTrusted := false
	for _, tp := range h.cfg.TrustedProxies {
		if tp != "" && peerIP == tp {
			isTrusted = true
			break
		}
	}

	if peerIP == "127.0.0.1" || peerIP == "::1" || peerIP == "bufconn" {
		isTrusted = true
	}

	if isTrusted {
		if md, ok := metadata.FromIncomingContext(ctx); ok {

			if xri := md.Get("x-real-ip"); len(xri) > 0 && xri[0] != "" {
				return strings.TrimSpace(xri[0])
			}

			if xff := md.Get("x-forwarded-for"); len(xff) > 0 {
				ips := strings.Split(xff[0], ",")
				if len(ips) > 0 {
					val := strings.TrimSpace(ips[len(ips)-1])
					if val != "" {
						return val
					}
				}
			}
		}
	}

	return peerIP
}

// extractUserAgent supplies client identity for audit trails when gRPC metadata includes it.
func (h *Handler) extractUserAgent(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ua := md.Get("user-agent"); len(ua) > 0 {
			return ua[0]
		}
	}
	return "grpc-client"
}

// requireAdminKey limits account provisioning to operators holding the shared admin secret.
func (h *Handler) requireAdminKey(ctx context.Context) error {
	if h.cfg == nil || h.cfg.AdminAPIKey == "" {
		return status.Error(codes.PermissionDenied, "admin credentials not configured")
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing credentials")
	}
	keys := md.Get(adminAPIKeyMetadata)
	if len(keys) == 0 || keys[0] == "" || keys[0] != string(h.cfg.AdminAPIKey) {
		return status.Error(codes.PermissionDenied, "admin credentials required")
	}
	return nil
}

// requireAuthUser binds bearer-gated RPCs to a verified principal before account state changes.
func (h *Handler) requireAuthUser(ctx context.Context) (db.User, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return db.User{}, status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	values := md.Get(authorizationHeaderKey)
	if len(values) == 0 {
		return db.User{}, status.Error(codes.Unauthenticated, "authorization header is not provided")
	}
	header := values[0]
	if len(header) < 7 || !strings.EqualFold(header[:7], "bearer ") {
		return db.User{}, status.Error(codes.Unauthenticated, "invalid authorization header format")
	}
	accessToken := strings.TrimSpace(header[7:])
	user, err := h.service.VerifyToken(ctx, accessToken)
	if err != nil {
		return db.User{}, mapError(err)
	}
	return user, nil
}

// userToPB omits password hashes and internal flags from outward-facing responses.
func userToPB(user db.User) *pb.User {
	return &pb.User{
		Id:         uuid.UUID(user.ID.Bytes).String(),
		Email:      user.Email,
		Role:       user.Role,
		CustomerId: uuid.UUID(user.CustomerID.Bytes).String(),
		CreatedAt:  timestamppb.New(user.CreatedAt.Time),
	}
}

// Register is admin-gated because self-registration is disabled in this deployment.
func (h *Handler) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if err := h.requireAdminKey(ctx); err != nil {
		return nil, err
	}

	var customerID uuid.UUID
	var err error
	if req.CustomerId != "" {
		customerID, err = uuid.Parse(req.CustomerId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid customer id")
		}
	}
	id, err := h.service.Register(ctx, RegisterDTO{
		Email:      req.Email,
		Password:   req.Password,
		Role:       req.Role,
		CustomerID: customerID,
	})
	if err != nil {
		return nil, mapError(err)
	}

	return &pb.RegisterResponse{
		UserId: id.String(),
	}, nil
}

// Login establishes sessions for programmatic clients that authenticate over gRPC.
func (h *Handler) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	duration := time.Duration(req.DurationHours) * time.Hour
	if duration <= 0 {
		duration = time.Duration(h.cfg.DefaultTokenDurationHrs) * time.Hour
	} else if duration > 24*time.Hour {
		duration = 24 * time.Hour
	}

	clientIP := h.extractClientIP(ctx)
	userAgent := h.extractUserAgent(ctx)

	resp, err := h.service.Login(ctx, req.Email, req.Password, userAgent, clientIP, duration)
	if err != nil {
		return nil, mapError(err)
	}

	return &resp, nil
}

// VerifyToken lets peer services validate tokens without holding signing keys locally.
func (h *Handler) VerifyToken(ctx context.Context, req *pb.VerifyTokenRequest) (*pb.VerifyTokenResponse, error) {
	user, err := h.service.VerifyToken(ctx, req.AccessToken)
	if err != nil {
		return nil, mapError(err)
	}

	return &pb.VerifyTokenResponse{
		User: userToPB(user),
	}, nil
}

// RefreshToken renews short-lived access tokens without forcing credential re-entry.
func (h *Handler) RefreshToken(ctx context.Context, req *pb.RefreshTokenRequest) (*pb.RefreshTokenResponse, error) {
	duration := time.Duration(h.cfg.DefaultTokenDurationHrs) * time.Hour
	accessToken, refreshToken, err := h.service.RefreshToken(ctx, req.RefreshToken, duration)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.RefreshTokenResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// RevokeToken supports logout and compromise response by ending the refresh chain.
func (h *Handler) RevokeToken(ctx context.Context, req *pb.RevokeTokenRequest) (*pb.RevokeTokenResponse, error) {
	err := h.service.RevokeToken(ctx, req.RefreshToken)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.RevokeTokenResponse{}, nil
}

// CreateAPIKey serves machine clients that need long-lived credentials outside browser sessions.
func (h *Handler) CreateAPIKey(ctx context.Context, req *pb.CreateAPIKeyRequest) (*pb.CreateAPIKeyResponse, error) {
	user, err := h.requireAuthUser(ctx)
	if err != nil {
		return nil, err
	}

	var expiresAt *time.Time
	if req.ExpiresAt != nil {
		t := req.ExpiresAt.AsTime()
		expiresAt = &t
	}

	userID := uuid.UUID(user.ID.Bytes)
	id, rawKey, err := h.service.CreateAPIKey(ctx, userID, req.Name, expiresAt)
	if err != nil {
		return nil, mapError(err)
	}

	resp := &pb.CreateAPIKeyResponse{
		Id:     id.String(),
		Name:   req.Name,
		RawKey: rawKey,
	}
	if expiresAt != nil {
		resp.ExpiresAt = timestamppb.New(*expiresAt)
	}
	return resp, nil
}

// ListAPIKeys lets owners audit active keys without receiving stored secrets again.
func (h *Handler) ListAPIKeys(ctx context.Context, _ *pb.ListAPIKeysRequest) (*pb.ListAPIKeysResponse, error) {
	user, err := h.requireAuthUser(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := h.service.ListUserAPIKeys(ctx, uuid.UUID(user.ID.Bytes))
	if err != nil {
		return nil, mapError(err)
	}

	keys := make([]*pb.APIKey, 0, len(rows))
	for _, row := range rows {
		key := &pb.APIKey{
			Id:        uuid.UUID(row.ID.Bytes).String(),
			Name:      row.Name,
			CreatedAt: timestamppb.New(row.CreatedAt.Time),
		}
		if row.ExpiresAt.Valid {
			key.ExpiresAt = timestamppb.New(row.ExpiresAt.Time)
		}
		keys = append(keys, key)
	}
	return &pb.ListAPIKeysResponse{Keys: keys}, nil
}

// ChangePassword ties credential rotation to the authenticated principal, not email alone.
func (h *Handler) ChangePassword(ctx context.Context, req *pb.ChangePasswordRequest) (*pb.ChangePasswordResponse, error) {
	user, err := h.requireAuthUser(ctx)
	if err != nil {
		return nil, err
	}

	clientIP := h.extractClientIP(ctx)
	userAgent := h.extractUserAgent(ctx)
	err = h.service.ChangePassword(ctx, uuid.UUID(user.ID.Bytes), req.OldPassword, req.NewPassword, clientIP, userAgent)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.ChangePasswordResponse{}, nil
}

// RequestEmailVerification starts ownership proof for accounts blocked at login until verified.
func (h *Handler) RequestEmailVerification(ctx context.Context, _ *pb.RequestEmailVerificationRequest) (*pb.RequestEmailVerificationResponse, error) {
	user, err := h.requireAuthUser(ctx)
	if err != nil {
		return nil, err
	}

	token, err := h.service.RequestEmailVerification(ctx, uuid.UUID(user.ID.Bytes))
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.RequestEmailVerificationResponse{VerificationToken: token}, nil
}

// ConfirmEmailVerification consumes a one-time token so email ownership claims cannot be replayed.
func (h *Handler) ConfirmEmailVerification(ctx context.Context, req *pb.ConfirmEmailVerificationRequest) (*pb.ConfirmEmailVerificationResponse, error) {
	_, err := h.service.ConfirmEmailVerification(ctx, req.VerificationToken)
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.ConfirmEmailVerificationResponse{}, nil
}

// mapError keeps client-facing codes and metrics stable while hiding store internals.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrRateLimitExceeded) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	if errors.Is(err, ErrInvalidCredentials) || errors.Is(err, ErrInvalidToken) || errors.Is(err, ErrExpiredToken) || errors.Is(err, ErrAccountLocked) || errors.Is(err, ErrSessionBlocked) || errors.Is(err, ErrEmailNotVerified) {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if errors.Is(err, ErrUserAlreadyExists) {
		return status.Error(codes.AlreadyExists, err.Error())
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "user not found")
	}
	if errors.Is(err, ErrValidation) || errors.Is(err, ErrPasswordReuse) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, "internal server error")
}

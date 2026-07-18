package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"runtime"
	"strings"
	"time"

	"espx/internal/auth/db"
	"espx/internal/auth/pb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Domain errors keep auth responses stable for clients, metrics, and gRPC mapping without leaking internals.
var (
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrAccountLocked      = errors.New("account locked due to too many failed attempts")
	ErrRateLimitExceeded  = errors.New("rate limit exceeded")
	ErrValidation         = errors.New("validation failed")
	ErrSessionBlocked     = errors.New("session is blocked")
	ErrPasswordReuse      = errors.New("password reuse not allowed")
	ErrEmailNotVerified   = errors.New("email not verified")
	ErrInvalidAPIKey      = errors.New("invalid api key")
)

// idempotentResultError carries a cached refresh rotation result out of a transaction without treating it as a failure.
type idempotentResultError struct {
	accessToken  string
	refreshToken string
}

// Error signals a duplicate refresh that already produced tokens so the caller can return them.
func (e *idempotentResultError) Error() string {
	return "idempotency hit"
}

// emailRegex rejects malformed registration and login addresses before they hit the database.
var (
	emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
)

// AuthLoginAttempts and AuthTokenErrors expose auth health signals for monitoring brute force and token failures.
var (
	AuthLoginAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "auth_login_attempts_total",
			Help: "Total number of login attempts",
		},
		[]string{"status", "failure_reason"},
	)
	AuthTokenErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "auth_token_validation_errors_total",
			Help: "Total number of token validation errors",
		},
		[]string{"error_type"},
	)
)

// init registers auth counters at startup so Prometheus scrapes see them from the first request.
func init() {
	prometheus.MustRegister(AuthLoginAttempts)
	prometheus.MustRegister(AuthTokenErrors)
}

// Service owns registration, login, token lifecycle, and credential policy for the auth service.
type Service struct {
	repo       db.Store
	tokenMaker Maker
	hasher     *PasswordHasher
	lockout    *LockoutLimiter
	rdb        redis.UniversalClient
	rehashSem  chan struct{}
	cryptoSem  chan struct{}
	mailer     Mailer
}

// NewService sizes crypto concurrency from Argon2 parallelism to avoid CPU saturation under login bursts.
func NewService(repo db.Store, tokenMaker Maker, hasher *PasswordHasher, lockout *LockoutLimiter, rdb redis.UniversalClient) *Service {
	gomaxprocs := runtime.GOMAXPROCS(0)
	p := 1
	if hasher != nil {
		if ph := hasher.GetParallelism(); ph > 0 {
			p = int(ph)
		}
	}
	cryptoLimit := gomaxprocs / p
	if cryptoLimit < 1 {
		cryptoLimit = 1
	}

	return &Service{
		repo:       repo,
		tokenMaker: tokenMaker,
		hasher:     hasher,
		lockout:    lockout,
		rdb:        rdb,
		rehashSem:  make(chan struct{}, 2),
		cryptoSem:  make(chan struct{}, cryptoLimit),
		mailer:     SlogMailer{},
	}
}

// SetMailer swaps the log-only default for a real provider before security notifications matter.
func (service *Service) SetMailer(mailer Mailer) {
	service.mailer = mailer
}

// RegisterDTO carries validated registration input from HTTP and gRPC adapters.
type RegisterDTO struct {
	Email      string
	Password   string
	Role       string
	CustomerID uuid.UUID
}

// Register seeds password history on creation so later rotations can detect reuse.
func (service *Service) Register(ctx context.Context, req RegisterDTO) (uuid.UUID, error) {
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))
	if !emailRegex.MatchString(req.Email) {
		slog.Warn("registration failed", slog.String("reason", "invalid email format"), slog.String("email", req.Email))
		return uuid.Nil, ErrValidation
	}
	if err := ValidatePassword(req.Password); err != nil {
		slog.Warn("registration failed", slog.String("reason", "invalid password"), slog.String("email", req.Email), slog.Any("error", err))
		return uuid.Nil, ErrValidation
	}

	role, err := ValidateRegisterRole(req.Role)
	if err != nil {
		slog.Warn("registration failed", slog.String("reason", "invalid role"), slog.String("email", req.Email), slog.String("role", req.Role))
		return uuid.Nil, ErrValidation
	}

	hashedPassword, err := service.hasher.HashPassword(req.Password)
	if err != nil {
		slog.Error("failed to hash password during registration", slog.String("email", req.Email), slog.Any("error", err))
		return uuid.Nil, err
	}

	arg := db.CreateUserParams{
		Email:         req.Email,
		PasswordHash:  hashedPassword,
		Role:          role,
		EmailVerified: true,
	}
	if req.CustomerID != uuid.Nil {
		arg.CustomerID.Bytes = req.CustomerID
		arg.CustomerID.Valid = true
	}

	var userID uuid.UUID
	err = service.repo.ExecTx(ctx, func(q db.Querier) error {
		userRow, err := q.CreateUser(ctx, arg)
		if err != nil {
			return err
		}
		userID = uuid.UUID(userRow.ID.Bytes)

		return q.CreatePasswordHistoryEntry(ctx, db.CreatePasswordHistoryEntryParams{
			UserID:       userRow.ID,
			PasswordHash: hashedPassword,
		})
	})

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			slog.Warn("registration failed", slog.String("reason", "user already exists"), slog.String("email", req.Email))
			return uuid.Nil, ErrUserAlreadyExists
		}
		slog.Error("registration failed", slog.String("email", req.Email), slog.Any("error", err))
		return uuid.Nil, err
	}

	return userID, nil
}

// LoginDTO groups login outputs for callers that need tokens plus the authenticated user record.
type LoginDTO struct {
	AccessToken  string
	RefreshToken string
	User         db.User
}

// Login centralizes lockout, constant-time verification, and session binding for every client.
func (service *Service) Login(ctx context.Context, email, password, userAgent, clientIP string, duration time.Duration) (pb.LoginResponse, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if !emailRegex.MatchString(email) {
		slog.Warn("login failed", slog.String("reason", "invalid email format"), slog.String("email", email), slog.String("ip", clientIP))
		return pb.LoginResponse{}, ErrValidation
	}

	if service.lockout != nil {
		allowedIP, errIP := service.lockout.AllowIP(ctx, clientIP, 20, time.Minute)
		if errIP != nil {
			AuthLoginAttempts.WithLabelValues("failure", "lockout_check_failed").Inc()
			slog.Error("failed to check ip rate limit in redis (fail-closed)", slog.String("ip", clientIP), slog.String("email", email), slog.Any("error", errIP))
			return pb.LoginResponse{}, ErrSessionBlocked
		}
		if !allowedIP {
			AuthLoginAttempts.WithLabelValues("failure", "ratelimit").Inc()
			slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "ip rate limit exceeded"))
			return pb.LoginResponse{}, ErrRateLimitExceeded
		}

		allowed, err := service.lockout.Allow(ctx, clientIP, email, 5, 15*time.Minute, 10*time.Minute)
		if err != nil {
			AuthLoginAttempts.WithLabelValues("failure", "lockout_check_failed").Inc()
			slog.Error("failed to check lockout in redis (fail-closed)", slog.String("ip", clientIP), slog.String("email", email), slog.Any("error", err))
			return pb.LoginResponse{}, ErrSessionBlocked
		}
		if allowed == 0 {
			AuthLoginAttempts.WithLabelValues("failure", "locked").Inc()
			slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "account locked by ip"))
			if service.mailer != nil {
				if mailErr := service.mailer.SendAccountLockedEmail(ctx, email, clientIP, "10 minutes"); mailErr != nil {
					slog.Error("failed to send account locked notification", slog.String("email", email), slog.Any("error", mailErr))
				}
			}
			return pb.LoginResponse{}, ErrAccountLocked
		}
		if allowed == -1 {
			AuthLoginAttempts.WithLabelValues("failure", "global_locked").Inc()
			slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "global account lockout triggered"))
			if service.mailer != nil {
				if mailErr := service.mailer.SendAccountLockedEmail(ctx, email, clientIP, "1 hour"); mailErr != nil {
					slog.Error("failed to send global account locked notification", slog.String("email", email), slog.Any("error", mailErr))
				}
			}

			return pb.LoginResponse{}, ErrAccountLocked
		}
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			if errDec := service.lockout.DecrementInflight(cleanupCtx, clientIP, email); errDec != nil {
				slog.Error("failed to decrement inflight count", slog.String("ip", clientIP), slog.String("email", email), slog.Any("error", errDec))
			}
			cancel()
		}()
	}

	var user db.User
	var userFound bool

	u, err := service.repo.GetUserByEmail(ctx, email)
	var hashToVerify string
	if err == nil {
		hashToVerify = u.PasswordHash
		userFound = true
		user = u
	} else {
		hashToVerify = service.hasher.GetDummyHash()
		userFound = false
	}

	select {
	case service.cryptoSem <- struct{}{}:
	case <-ctx.Done():
		return pb.LoginResponse{}, ctx.Err()
	default:
		AuthLoginAttempts.WithLabelValues("failure", "ratelimit").Inc()
		slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "server crypto load limit exceeded"))
		return pb.LoginResponse{}, ErrRateLimitExceeded
	}
	defer func() { <-service.cryptoSem }()

	match, verifyErr := VerifyPassword(password, hashToVerify)

	if !userFound || (verifyErr != nil && !errors.Is(verifyErr, ErrInsecureHashParameters)) || !match {
		AuthLoginAttempts.WithLabelValues("failure", "invalid_credentials").Inc()
		slog.Warn("security_audit_event", slog.String("event", "auth_failure"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "invalid credentials"))
		if service.lockout != nil {
			res, errInc := service.lockout.Increment(ctx, clientIP, email, 5, 15*time.Minute, 10*time.Minute)
			if errInc != nil {
				slog.Error("failed to increment lockout count", slog.String("ip", clientIP), slog.String("email", email), slog.Any("error", errInc))
			} else if res == -1 {

				slog.Warn("security_audit_event", slog.String("event", "global_lockout_increment"), slog.String("ip", clientIP), slog.String("email", email), slog.String("reason", "global lockout increment reached limit"))
			}
		}
		return pb.LoginResponse{}, ErrInvalidCredentials
	}

	if errors.Is(verifyErr, ErrInsecureHashParameters) {
		lockKey := "lock:rehash:" + email
		ok, errLock := service.rdb.SetNX(ctx, lockKey, "1", time.Minute).Result()
		if errLock != nil {
			slog.Error("failed to acquire rehash lock", slog.String("email", email), slog.Any("error", errLock))
		}
		if ok {
			select {
			case service.rehashSem <- struct{}{}:
				go func(plainPwd, userEmail string) {
					defer func() {
						<-service.rehashSem
						cleanupCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
						if errDel := service.rdb.Del(cleanupCtx, lockKey).Err(); errDel != nil {
							slog.Error("failed to release rehash lock", slog.String("email", userEmail), slog.Any("error", errDel))
						}
						cancel()
					}()
					newHash, errHash := service.hasher.HashPassword(plainPwd)
					if errHash != nil {
						slog.Error("failed to hash password during rehash", slog.String("email", userEmail), slog.Any("error", errHash))
						return
					}
					if errUpd := service.repo.UpdatePassword(context.Background(), db.UpdatePasswordParams{
						Email:        userEmail,
						PasswordHash: newHash,
					}); errUpd != nil {
						slog.Error("failed to update rehashed password", slog.String("email", userEmail), slog.Any("error", errUpd))
					}
				}(password, email)
			default:
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				if errDel := service.rdb.Del(cleanupCtx, lockKey).Err(); errDel != nil {
					slog.Error("failed to release rehash lock on default", slog.String("email", email), slog.Any("error", errDel))
				}
				cancel()
			}
		}
	}

	if service.lockout != nil {
		if errReset := service.lockout.Reset(ctx, clientIP, email); errReset != nil {
			slog.Error("failed to reset lockout status", slog.String("ip", clientIP), slog.String("email", email), slog.Any("error", errReset))
		}
	}

	if user.IsBlocked {
		return pb.LoginResponse{}, ErrSessionBlocked
	}
	if !user.EmailVerified {
		AuthLoginAttempts.WithLabelValues("failure", "email_not_verified").Inc()
		return pb.LoginResponse{}, ErrEmailNotVerified
	}

	refreshTokenId := uuid.Must(uuid.NewV7())

	accessToken, err := service.tokenMaker.CreateToken(
		uuid.UUID(user.ID.Bytes),
		refreshTokenId,
		user.Role,
		uuid.UUID(user.CustomerID.Bytes),
		duration,
	)
	if err != nil {
		AuthLoginAttempts.WithLabelValues("failure", "error").Inc()
		return pb.LoginResponse{}, err
	}

	AuthLoginAttempts.WithLabelValues("success", "").Inc()

	refreshTokenStr := uuid.NewString()
	expiresAt := time.Now().Add(7 * 24 * time.Hour)

	err = service.repo.ExecTx(ctx, func(q db.Querier) error {
		if _, err = q.CreateSession(ctx, db.CreateSessionParams{
			ID:           pgtype.UUID{Bytes: refreshTokenId, Valid: true},
			UserID:       user.ID,
			RefreshToken: refreshTokenStr,
			UserAgent:    userAgent,
			ClientIp:     clientIP,
			IsBlocked:    false,
			ExpiresAt:    pgtype.Timestamptz{Time: expiresAt, Valid: true},
		}); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		slog.Error("failed to create session", slog.String("email", user.Email), slog.Any("error", err))
		return pb.LoginResponse{}, err
	}

	service.notifyNewIPLogin(ctx, user, clientIP, userAgent)

	return pb.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshTokenStr,
		User: &pb.User{
			Id:         uuid.UUID(user.ID.Bytes).String(),
			Email:      user.Email,
			Role:       user.Role,
			CustomerId: uuid.UUID(user.CustomerID.Bytes).String(),
			CreatedAt:  timestamppb.New(user.CreatedAt.Time),
		},
	}, nil
}

// VerifyToken rejects revoked or blocked principals before downstream services trust the token.
func (service *Service) VerifyToken(ctx context.Context, accessToken string) (db.User, error) {
	payload, err := service.tokenMaker.VerifyToken(accessToken)
	if err != nil {
		AuthTokenErrors.WithLabelValues("invalid").Inc()
		return db.User{}, err
	}

	if service.rdb != nil {
		revoked, errRev := CheckTokenRevocation(ctx, service.rdb, payload)
		if errRev != nil {
			AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
			slog.Error("failed to check token revocation in redis (fail-closed)", slog.Any("error", errRev))
			return db.User{}, ErrSessionBlocked
		}
		if revoked {
			return db.User{}, ErrSessionBlocked
		}
	}

	user, err := service.repo.GetUserByID(ctx, pgtype.UUID{Bytes: payload.UserID, Valid: true})
	if err != nil {
		AuthTokenErrors.WithLabelValues("user_lookup_failed").Inc()
		slog.Error("failed to load user during verify (fail-closed)", slog.Any("error", err))
		return db.User{}, ErrSessionBlocked
	}

	if user.IsBlocked {
		return db.User{}, ErrSessionBlocked
	}
	if !user.EmailVerified {
		return db.User{}, ErrEmailNotVerified
	}

	return user, nil
}

// RefreshToken blocks refresh-token reuse and caches rotation results for concurrent retries.
func (service *Service) RefreshToken(ctx context.Context, refreshTokenStr string, duration time.Duration) (string, string, error) {
	if service.rdb != nil {
		cached, err := service.rdb.Get(ctx, "idempotency:refresh:"+refreshTokenStr).Result()
		if err == nil && cached != "" {
			parts := strings.Split(cached, " ")
			if len(parts) == 2 {
				return parts[0], parts[1], nil
			}
		}
	}

	var accessToken string
	var newRefreshTokenStr string

	err := service.repo.ExecTx(ctx, func(q db.Querier) error {
		session, err := q.GetSessionByRefreshTokenForUpdate(ctx, refreshTokenStr)
		if err != nil {
			slog.Warn("refresh token failed", slog.String("reason", "invalid refresh token"), slog.Any("error", err))
			return ErrInvalidToken
		}

		if session.IsBlocked {
			if service.rdb != nil {
				cached, errCached := service.rdb.Get(ctx, "idempotency:refresh:"+refreshTokenStr).Result()
				if errCached == nil && cached != "" {
					parts := strings.Split(cached, " ")
					if len(parts) == 2 {
						return &idempotentResultError{
							accessToken:  parts[0],
							refreshToken: parts[1],
						}
					}
				}
			}
			slog.Warn("refresh token failed", slog.String("reason", "session is blocked"), slog.String("session_id", uuid.UUID(session.ID.Bytes).String()))
			return ErrSessionBlocked
		}

		if session.ExpiresAt.Time.Before(time.Now()) {
			slog.Warn("refresh token failed", slog.String("reason", "refresh token expired"), slog.String("session_id", uuid.UUID(session.ID.Bytes).String()))
			return ErrExpiredToken
		}

		user, err := q.GetUserByID(ctx, session.UserID)
		if err != nil {
			slog.Error("refresh token failed", slog.String("reason", "user not found"), slog.Any("error", err))
			return ErrInvalidToken
		}

		if user.IsBlocked {
			slog.Warn("refresh token failed", slog.String("reason", "user is blocked"), slog.String("email", user.Email))
			return ErrSessionBlocked
		}
		if !user.EmailVerified {
			return ErrEmailNotVerified
		}

		err = q.BlockSession(ctx, session.ID)
		if err != nil {
			slog.Error("refresh token failed", slog.String("reason", "failed to block old session"), slog.Any("error", err))
			return err
		}

		newRefreshTokenId := uuid.Must(uuid.NewV7())

		accessToken, err = service.tokenMaker.CreateToken(
			uuid.UUID(user.ID.Bytes),
			newRefreshTokenId,
			user.Role,
			uuid.UUID(user.CustomerID.Bytes),
			duration,
		)
		if err != nil {
			slog.Error("refresh token failed", slog.String("reason", "failed to create access token"), slog.Any("error", err))
			return err
		}

		newRefreshTokenStr = uuid.NewString()
		expiresAt := time.Now().Add(7 * 24 * time.Hour)

		if _, err = q.CreateSession(ctx, db.CreateSessionParams{
			ID:           pgtype.UUID{Bytes: newRefreshTokenId, Valid: true},
			UserID:       user.ID,
			RefreshToken: newRefreshTokenStr,
			UserAgent:    session.UserAgent,
			ClientIp:     session.ClientIp,
			IsBlocked:    false,
			ExpiresAt:    pgtype.Timestamptz{Time: expiresAt, Valid: true},
		}); err != nil {
			slog.Error("refresh token failed", slog.String("reason", "failed to create new session"), slog.Any("error", err))
			return err
		}

		return nil
	})

	if err != nil {
		var idmpErr *idempotentResultError
		if errors.As(err, &idmpErr) {
			return idmpErr.accessToken, idmpErr.refreshToken, nil
		}
		return "", "", err
	}

	if service.rdb != nil {
		if errSet := service.rdb.Set(ctx, "idempotency:refresh:"+refreshTokenStr, accessToken+" "+newRefreshTokenStr, 5*time.Minute).Err(); errSet != nil {
			slog.Error("failed to set idempotency cache", slog.Any("error", errSet))
		}
	}

	return accessToken, newRefreshTokenStr, nil
}

// RevokeToken ends the refresh chain and marks in-flight access tokens via Redis before session block.
func (service *Service) RevokeToken(ctx context.Context, refreshTokenStr string) error {
	session, err := service.repo.GetSessionByRefreshToken(ctx, refreshTokenStr)
	if err == nil && service.rdb != nil {
		sessionID := uuid.UUID(session.ID.Bytes).String()
		ttl := 24 * time.Hour
		if session.ExpiresAt.Valid {
			ttl = time.Until(session.ExpiresAt.Time)
		}
		if ttl <= 0 {
			ttl = 24 * time.Hour
		}
		if errSet := service.rdb.Set(ctx, "revoked:session:"+sessionID, "1", ttl).Err(); errSet != nil {
			slog.Error("failed to set revoked session in redis", slog.String("session_id", sessionID), slog.Any("error", errSet))
		}
	}
	return service.repo.BlockSessionByRefreshToken(ctx, refreshTokenStr)
}

// AuditLog must never break primary auth flows when the audit store is down.
func (service *Service) AuditLog(ctx context.Context, userID uuid.UUID, action, targetType, targetID, clientIP, userAgent string, changes, metadata map[string]any) {
	changesJSON, err := json.Marshal(changes)
	if err != nil {
		slog.Error("failed to marshal audit log changes", "error", err)
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		slog.Error("failed to marshal audit log metadata", "error", err)
	}

	var uid pgtype.UUID
	if userID != uuid.Nil {
		uid = pgtype.UUID{Bytes: userID, Valid: true}
	}

	_, err = service.repo.CreateAuthAuditLog(ctx, db.CreateAuthAuditLogParams{
		UserID:     uid,
		Action:     action,
		TargetType: pgtype.Text{String: targetType, Valid: targetType != ""},
		TargetID:   pgtype.Text{String: targetID, Valid: targetID != ""},
		ClientIp:   pgtype.Text{String: clientIP, Valid: clientIP != ""},
		UserAgent:  pgtype.Text{String: userAgent, Valid: userAgent != ""},
		Changes:    changesJSON,
		Metadata:   metadataJSON,
	})
	if err != nil {
		slog.Error("failed to write auth audit log (non-fatal)",
			slog.String("action", action),
			slog.Any("user_id", userID),
			slog.Any("error", err))
	}
}

// ChangePassword enforces reuse policy and alerts the owner because stolen sessions may trigger rotation.
func (service *Service) ChangePassword(ctx context.Context, userID uuid.UUID, oldPassword, newPassword, clientIP, userAgent string) error {
	if err := ValidatePassword(newPassword); err != nil {
		return ErrValidation
	}

	user, err := service.repo.GetUserByID(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	if err != nil {
		return err
	}

	select {
	case service.cryptoSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrRateLimitExceeded
	}
	defer func() { <-service.cryptoSem }()

	match, verifyErr := VerifyPassword(oldPassword, user.PasswordHash)
	if !match || (verifyErr != nil && !errors.Is(verifyErr, ErrInsecureHashParameters)) {
		service.AuditLog(ctx, userID, "PASSWORD_CHANGE_FAILED", "user", userID.String(), clientIP, userAgent,
			map[string]any{"reason": "old_password_mismatch"}, nil)
		return ErrInvalidCredentials
	}

	historyHashes, err := service.repo.GetPasswordHistory(ctx, db.GetPasswordHistoryParams{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
		Limit:  3,
	})
	if err != nil {
		return err
	}

	for _, oldHash := range historyHashes {
		matchHist, verifyHistErr := VerifyPassword(newPassword, oldHash)
		if verifyHistErr != nil && !errors.Is(verifyHistErr, ErrInsecureHashParameters) {
			return verifyHistErr
		}
		if matchHist {
			service.AuditLog(ctx, userID, "PASSWORD_REUSE_REJECTED", "user", userID.String(), clientIP, userAgent,
				map[string]any{"reason": "password_reuse_detected"}, nil)
			return ErrPasswordReuse
		}
	}

	newHash, err := service.hasher.HashPassword(newPassword)
	if err != nil {
		return err
	}

	err = service.repo.ExecTx(ctx, func(q db.Querier) error {
		if err := q.UpdatePassword(ctx, db.UpdatePasswordParams{
			Email:        user.Email,
			PasswordHash: newHash,
		}); err != nil {
			return err
		}

		return q.CreatePasswordHistoryEntry(ctx, db.CreatePasswordHistoryEntryParams{
			UserID:       pgtype.UUID{Bytes: userID, Valid: true},
			PasswordHash: newHash,
		})
	})
	if err != nil {
		return err
	}

	service.AuditLog(ctx, userID, "PASSWORD_CHANGED", "user", userID.String(), clientIP, userAgent, nil, nil)

	if mailErr := service.mailer.SendPasswordChangedEmail(ctx, user.Email, clientIP, userAgent); mailErr != nil {
		slog.Error("failed to send password changed notification email", "user_id", userID, "error", mailErr)
	}

	return nil
}

// CreateAPIKey returns the raw secret once because only a hash is persisted.
func (service *Service) CreateAPIKey(ctx context.Context, userID uuid.UUID, name string, expiresAt *time.Time) (id uuid.UUID, rawKey string, err error) {

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return uuid.Nil, "", err
	}
	rawKey = base64.RawURLEncoding.EncodeToString(raw)

	keyHash, err := service.hasher.HashPassword(rawKey)
	if err != nil {
		return uuid.Nil, "", err
	}

	var exp pgtype.Timestamptz
	if expiresAt != nil {
		exp = pgtype.Timestamptz{Time: *expiresAt, Valid: true}
	}

	row, err := service.repo.CreateAPIKey(ctx, db.CreateAPIKeyParams{
		KeyHash:   keyHash,
		KeyLookup: pgtype.Text{String: apiKeyLookup(rawKey), Valid: true},
		UserID:    pgtype.UUID{Bytes: userID, Valid: true},
		Name:      name,
		ExpiresAt: exp,
	})
	if err != nil {
		return uuid.Nil, "", err
	}

	id = uuid.UUID(row.ID.Bytes)
	service.AuditLog(ctx, userID, "API_KEY_CREATED", "api_key", id.String(), "", "",
		map[string]any{"name": name, "expires_at": expiresAt}, nil)
	return id, rawKey, nil
}

// ListUserAPIKeys exposes metadata only; stored secrets are never retrievable after creation.
func (service *Service) ListUserAPIKeys(ctx context.Context, userID uuid.UUID) ([]db.ListUserAPIKeysRow, error) {
	return service.repo.ListUserAPIKeys(ctx, pgtype.UUID{Bytes: userID, Valid: true})
}

// VerifyAPIKey resolves a presented secret to the owning user via lookup index and argon2 verification.
func (service *Service) VerifyAPIKey(ctx context.Context, rawKey string) (db.User, error) {
	rawKey = strings.TrimSpace(rawKey)
	if rawKey == "" {
		return db.User{}, ErrInvalidAPIKey
	}

	row, err := service.repo.GetAPIKeyByLookup(ctx, pgtype.Text{String: apiKeyLookup(rawKey), Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.User{}, ErrInvalidAPIKey
		}
		return db.User{}, err
	}

	select {
	case service.cryptoSem <- struct{}{}:
	case <-ctx.Done():
		return db.User{}, ctx.Err()
	default:
		return db.User{}, ErrRateLimitExceeded
	}
	defer func() { <-service.cryptoSem }()

	match, err := VerifyPassword(rawKey, row.KeyHash)
	if err != nil || !match {
		return db.User{}, ErrInvalidAPIKey
	}

	user, err := service.repo.GetUserByID(ctx, row.UserID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.User{}, ErrInvalidAPIKey
		}
		return db.User{}, err
	}
	if user.IsBlocked {
		return db.User{}, ErrAccountLocked
	}
	return user, nil
}

// RequestEmailVerification stores a short-lived token in Redis to prove mailbox ownership out of band.
func (service *Service) RequestEmailVerification(ctx context.Context, userID uuid.UUID) (string, error) {
	token := uuid.NewString()
	key := "auth:email_verify:" + token
	if err := service.rdb.Set(ctx, key, userID.String(), 24*time.Hour).Err(); err != nil {
		return "", err
	}
	service.AuditLog(ctx, userID, "EMAIL_VERIFICATION_REQUESTED", "user", userID.String(), "", "", nil, nil)
	return token, nil
}

// ConfirmEmailVerification deletes the token on use so ownership claims cannot be replayed.
func (service *Service) ConfirmEmailVerification(ctx context.Context, token string) (uuid.UUID, error) {
	key := "auth:email_verify:" + token
	userIDStr, err := service.rdb.Get(ctx, key).Result()
	if err != nil {
		return uuid.Nil, ErrInvalidToken
	}

	if delErr := service.rdb.Del(ctx, key).Err(); delErr != nil {
		slog.Warn("failed to delete email verification token from Redis", "token", token, "error", delErr)
	}

	uid, err := uuid.Parse(userIDStr)
	if err != nil {
		return uuid.Nil, err
	}

	if err := service.repo.SetEmailVerified(ctx, pgtype.UUID{Bytes: uid, Valid: true}); err != nil {
		return uuid.Nil, err
	}
	service.AuditLog(ctx, uid, "EMAIL_VERIFIED", "user", uid.String(), "", "", nil, nil)
	return uid, nil
}

// BlockUser must invalidate in-flight access tokens because Postgres block alone is not checked on every request.
func (service *Service) BlockUser(ctx context.Context, email string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	user, err := service.repo.GetUserByEmail(ctx, email)
	if err != nil {
		return err
	}
	if err := service.repo.BlockUser(ctx, email); err != nil {
		return err
	}
	userID := uuid.UUID(user.ID.Bytes)
	if err := RevokeUserAccess(ctx, service.rdb, userID, defaultUserRevocationTTL); err != nil {
		slog.Error("failed to publish user revocation marker", slog.String("email", email), slog.Any("error", err))
	}
	service.AuditLog(ctx, userID, "USER_BLOCKED", "user", userID.String(), "", "", nil, nil)
	return nil
}

// UnblockUser clears the Redis marker so restored accounts are not rejected by stale revocation state.
func (service *Service) UnblockUser(ctx context.Context, email string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	user, err := service.repo.GetUserByEmail(ctx, email)
	if err != nil {
		return err
	}
	if err := service.repo.UnblockUser(ctx, email); err != nil {
		return err
	}
	userID := uuid.UUID(user.ID.Bytes)
	if err := ClearUserRevocation(ctx, service.rdb, userID); err != nil {
		slog.Error("failed to clear user revocation marker", slog.String("email", email), slog.Any("error", err))
	}
	service.AuditLog(ctx, userID, "USER_UNBLOCKED", "user", userID.String(), "", "", nil, nil)
	return nil
}

// notifyNewIPLogin warns owners about credential use from a new network location.
func (service *Service) notifyNewIPLogin(ctx context.Context, user db.User, clientIP, userAgent string) {
	if service.rdb == nil || service.mailer == nil || clientIP == "" || clientIP == "unknown" {
		return
	}
	userID := uuid.UUID(user.ID.Bytes).String()
	knownKey := "auth:known_ips:" + userID
	added, err := service.rdb.SAdd(ctx, knownKey, clientIP).Result()
	if err != nil {
		slog.Error("failed to record known login IP", slog.String("user_id", userID), slog.Any("error", err))
		return
	}
	if added == 0 {
		return
	}
	count, err := service.rdb.SCard(ctx, knownKey).Result()
	if err != nil || count <= 1 {
		return
	}
	if mailErr := service.mailer.SendNewIPLoginEmail(ctx, user.Email, clientIP, userAgent); mailErr != nil {
		slog.Error("failed to send new IP login notification", slog.String("email", user.Email), slog.Any("error", mailErr))
	}
}

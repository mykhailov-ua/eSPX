package auth

import ()

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type contextKey string

const (
	AuthorizationPayloadKey contextKey = "authorization_payload"
)

const (
	authorizationHeaderKey  = "authorization"
	authorizationTypeBearer = "bearer"
)

func AuthMiddleware(tokenMaker Maker, rdb redis.UniversalClient, allowedRoles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authorizationHeader := r.Header.Get(authorizationHeaderKey)
			if len(authorizationHeader) == 0 {
				err := errors.New("authorization header is not provided")
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			fields := strings.Fields(authorizationHeader)
			if len(fields) < 2 {
				err := errors.New("invalid authorization header format")
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			authorizationType := strings.ToLower(fields[0])
			if authorizationType != authorizationTypeBearer {
				err := fmt.Errorf("unsupported authorization type %s", authorizationType)
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			accessToken := fields[1]
			payload, err := tokenMaker.VerifyToken(accessToken)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			if rdb != nil {
				ctxRevoked, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
				cmds, errPipe := rdb.Pipelined(ctxRevoked, func(pipe redis.Pipeliner) error {
					pipe.Exists(ctxRevoked, "revoked:token:"+payload.ID.String())
					pipe.Exists(ctxRevoked, "revoked:session:"+payload.SessionID.String())
					pipe.Exists(ctxRevoked, "revoked:user:"+payload.UserID.String())
					return nil
				})
				cancel()

				if errPipe != nil {
					AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
					slog.Error("failed to check token revocation in redis (fail-closed)", slog.Any("error", errPipe))
					http.Error(w, "authorization check failed", http.StatusUnauthorized)
					return
				}

				if len(cmds) != 3 {
					AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
					slog.Error("unexpected pipeline commands count in redis (fail-closed)", slog.Int("expected", 3), slog.Int("got", len(cmds)))
					http.Error(w, "authorization check failed", http.StatusUnauthorized)
					return
				}

				for _, cmd := range cmds {
					intCmd, ok := cmd.(*redis.IntCmd)
					if !ok {
						AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
						slog.Error("unexpected command type in redis pipeline (fail-closed)")
						http.Error(w, "authorization check failed", http.StatusUnauthorized)
						return
					}
					exists, errExists := intCmd.Result()
					if errExists != nil {
						AuthTokenErrors.WithLabelValues("revocation_check_failed").Inc()
						slog.Error("failed to get pipeline result in redis (fail-closed)", slog.Any("error", errExists))
						http.Error(w, "authorization check failed", http.StatusUnauthorized)
						return
					}
					if exists > 0 {
						http.Error(w, "token revoked", http.StatusUnauthorized)
						return
					}
				}
			}

			if len(allowedRoles) > 0 {
				authorized := false
				for _, role := range allowedRoles {
					if payload.Role == role {
						authorized = true
						break
					}
				}
				if !authorized {
					http.Error(w, "permission denied", http.StatusForbidden)
					return
				}
			}

			ctx := context.WithValue(r.Context(), AuthorizationPayloadKey, payload)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func GetPayload(ctx context.Context) (*Payload, error) {
	payload, ok := ctx.Value(AuthorizationPayloadKey).(*Payload)
	if !ok {
		return nil, errors.New("context does not contain authorization payload")
	}
	return payload, nil
}

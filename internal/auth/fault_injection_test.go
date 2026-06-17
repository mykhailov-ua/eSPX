package auth

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	authChaosWorkers  = 20
	authChaosAttempts = 10
)

func requireAuthFaultActive(t *testing.T, faultActive func() bool, msg string) {
	t.Helper()
	require.Eventually(t, faultActive, 10*time.Second, 100*time.Millisecond, msg)
}

func countVerifyFailClosed(ctx context.Context, svc *Service, accessToken string, attempts int) int {
	n := 0
	for i := 0; i < attempts; i++ {
		_, err := svc.VerifyToken(ctx, accessToken)
		if errors.Is(err, ErrSessionBlocked) {
			n++
		}
	}
	return n
}

func countLoginBlocked(ctx context.Context, svc *Service, email, password string, attempts int) int {
	n := 0
	for i := 0; i < attempts; i++ {
		clientIP := fmt.Sprintf("10.2.0.%d", i+1)
		_, err := svc.Login(ctx, email, password, "chaos-agent", clientIP, time.Hour)
		if errors.Is(err, ErrSessionBlocked) || errors.Is(err, ErrInvalidCredentials) {
			n++
		}
	}
	return n
}

// TestChaos_AuthRedisTerminateFailClosedVerify kills Redis and proves VerifyToken fails closed.
func TestChaos_AuthRedisTerminateFailClosedVerify(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "chaos-redis-verify@example.com"
	password := "SuperSecure123!"

	_, accessToken, _ := infra.registerAndLogin(t, svc, email, password)

	_, err := svc.VerifyToken(ctx, accessToken)
	require.NoError(t, err, "baseline: verify must succeed before fault")

	require.NoError(t, infra.RedisContainer.Terminate(ctx))
	requireAuthFaultActive(t, func() bool {
		return countVerifyFailClosed(ctx, svc, accessToken, 3) >= 2
	}, "VerifyToken must fail closed once Redis is dead")

	failClosed := countVerifyFailClosed(ctx, svc, accessToken, authChaosAttempts)

	logChaosProof(t, "redis_terminate", map[string]string{
		"subsystem":    "auth",
		"op":           "verify_token",
		"baseline_ok":  "true",
		"fail_closed":  strconv.Itoa(failClosed) + "/" + strconv.Itoa(authChaosAttempts),
		"fault_verify": "redis_container_terminated",
	})
	require.Equal(t, authChaosAttempts, failClosed,
		"VerifyToken must fail closed when Redis is dead, got %d/%d", failClosed, authChaosAttempts)
}

// TestChaos_AuthPGTerminateBlocksLogin terminates Postgres and proves login cannot succeed.
func TestChaos_AuthPGTerminateBlocksLogin(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "chaos-pg-login@example.com"
	password := "SuperSecure123!"

	_, _, _ = infra.registerAndLogin(t, svc, email, password)

	require.NoError(t, infra.PGContainer.Terminate(ctx))
	requireAuthFaultActive(t, func() bool {
		return infra.Pool.Ping(ctx) != nil
	}, "pg ping must fail after container terminate")

	loginFails := countLoginBlocked(ctx, svc, email, password, 8)

	logChaosProof(t, "postgres_terminate", map[string]string{
		"subsystem":      "auth",
		"op":             "login",
		"baseline_ok":    "true",
		"pg_ping_failed": "true",
		"login_failed":   strconv.Itoa(loginFails) + "/8",
		"fault_verify":   "postgres_container_terminated",
	})
	require.Equal(t, 8, loginFails,
		"login must deny when Postgres is dead, got %d/8", loginFails)
}

// TestChaos_AuthPGDownVerifyTokenFailClosed stops Postgres while Redis stays up and proves VerifyToken fails closed.
func TestChaos_AuthPGDownVerifyTokenFailClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "chaos-pg-verify@example.com"
	password := "SuperSecure123!"

	_, accessToken, _ := infra.registerAndLogin(t, svc, email, password)
	_, err := svc.VerifyToken(ctx, accessToken)
	require.NoError(t, err, "baseline: verify must succeed before fault")

	stopAuthContainer(t, infra.PGContainer)
	requireAuthFaultActive(t, func() bool {
		return infra.Pool.Ping(ctx) != nil
	}, "pg ping must fail after stop")

	failClosed := countVerifyFailClosed(ctx, svc, accessToken, authChaosAttempts)

	logChaosProof(t, "postgres_stop_verify", map[string]string{
		"subsystem":    "auth",
		"op":           "verify_token",
		"baseline_ok":  "true",
		"redis_alive":  "true",
		"fail_closed":  strconv.Itoa(failClosed) + "/" + strconv.Itoa(authChaosAttempts),
		"fault_verify": "postgres_container_stopped",
	})
	require.Equal(t, authChaosAttempts, failClosed,
		"VerifyToken must fail closed when Postgres is down, got %d/%d", failClosed, authChaosAttempts)
}

// TestChaos_AuthRedisDownLockoutFailClosed stops Redis while Postgres stays up and proves login cannot bypass lockout.
func TestChaos_AuthRedisDownLockoutFailClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "chaos-redis-lockout@example.com"
	password := "SuperSecure123!"

	_, _, _ = infra.registerAndLogin(t, svc, email, password)

	stopAuthContainer(t, infra.RedisContainer)
	requireAuthFaultActive(t, func() bool {
		return infra.Redis.Ping(ctx).Err() != nil
	}, "redis ping must fail after stop")

	blocked := 0
	const attempts = 8
	for i := 0; i < attempts; i++ {
		clientIP := fmt.Sprintf("10.3.0.%d", i+1)
		_, err := svc.Login(ctx, email, password, "chaos-agent", clientIP, time.Hour)
		if errors.Is(err, ErrSessionBlocked) {
			blocked++
		}
	}

	logChaosProof(t, "redis_stop_lockout", map[string]string{
		"subsystem":      "auth",
		"op":             "login",
		"baseline_ok":    "true",
		"postgres_alive": "true",
		"fail_closed":    strconv.Itoa(blocked) + "/" + strconv.Itoa(attempts),
		"fault_verify":   "redis_container_stopped",
	})
	require.Equal(t, attempts, blocked,
		"login must fail closed when lockout store is unreachable, got %d/%d", blocked, attempts)
}

// TestChaos_AuthRedisStopStartRecovery stops Redis, proves deny, then proves VerifyToken recovers.
func TestChaos_AuthRedisStopStartRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "chaos-redis-recovery@example.com"
	password := "SuperSecure123!"

	_, accessToken, _ := infra.registerAndLogin(t, svc, email, password)
	_, err := svc.VerifyToken(ctx, accessToken)
	require.NoError(t, err, "baseline: verify must succeed before fault")

	stopAuthContainer(t, infra.RedisContainer)
	requireAuthFaultActive(t, func() bool {
		return countVerifyFailClosed(ctx, svc, accessToken, 3) >= 2
	}, "VerifyToken must fail closed while Redis is stopped")

	startAuthContainer(t, infra.RedisContainer)
	infra.refreshRedisClient(t)
	svc = infra.newService(t)

	recovered := false
	require.Eventually(t, func() bool {
		_, err := svc.VerifyToken(ctx, accessToken)
		recovered = err == nil
		return recovered
	}, 30*time.Second, 200*time.Millisecond, "VerifyToken must recover after Redis restart")

	logChaosProof(t, "redis_stop_start_recovery", map[string]string{
		"subsystem":    "auth",
		"op":           "verify_token",
		"baseline_ok":  "true",
		"recovered":    strconv.FormatBool(recovered),
		"fault_verify": "redis_container_stopped_then_started",
	})
}

// TestChaos_AuthPGStopStartRecovery stops Postgres, proves login deny, then proves login recovers.
func TestChaos_AuthPGStopStartRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "chaos-pg-recovery@example.com"
	password := "SuperSecure123!"

	_, _, _ = infra.registerAndLogin(t, svc, email, password)

	stopAuthContainer(t, infra.PGContainer)
	requireAuthFaultActive(t, func() bool {
		_, err := svc.Login(ctx, email, password, "chaos-agent", "10.4.0.1", time.Hour)
		return err != nil
	}, "login must fail while Postgres is stopped")

	startAuthContainer(t, infra.PGContainer)
	infra.refreshPGPool(t)
	svc = infra.newService(t)

	recovered := false
	require.Eventually(t, func() bool {
		_, err := svc.Login(ctx, email, password, "chaos-agent", "10.4.0.2", time.Hour)
		recovered = err == nil
		return recovered
	}, 30*time.Second, 200*time.Millisecond, "login must recover after Postgres restart")

	logChaosProof(t, "postgres_stop_start_recovery", map[string]string{
		"subsystem":    "auth",
		"op":           "login",
		"baseline_ok":  "true",
		"recovered":    strconv.FormatBool(recovered),
		"fault_verify": "postgres_container_stopped_then_started",
	})
}

// TestChaos_AuthConcurrentVerifyDuringRedisOutage hammers VerifyToken while Redis is dead.
func TestChaos_AuthConcurrentVerifyDuringRedisOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "chaos-redis-concurrent@example.com"
	password := "SuperSecure123!"

	_, accessToken, _ := infra.registerAndLogin(t, svc, email, password)

	stopAuthContainer(t, infra.RedisContainer)
	requireAuthFaultActive(t, func() bool {
		return infra.Redis.Ping(ctx).Err() != nil
	}, "redis ping must fail after stop")

	var failClosed atomic.Int32
	var wg sync.WaitGroup
	wg.Add(authChaosWorkers)
	for i := 0; i < authChaosWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				_, err := svc.VerifyToken(ctx, accessToken)
				if errors.Is(err, ErrSessionBlocked) {
					failClosed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	total := int(failClosed.Load())
	expected := authChaosWorkers * 5

	logChaosProof(t, "redis_stop_concurrent_verify", map[string]string{
		"subsystem":    "auth",
		"op":           "verify_token",
		"workers":      strconv.Itoa(authChaosWorkers),
		"fail_closed":  strconv.Itoa(total) + "/" + strconv.Itoa(expected),
		"fault_verify": "redis_container_stopped_concurrent",
	})
	require.Equal(t, expected, total,
		"concurrent VerifyToken must fail closed under Redis outage, got %d/%d", total, expected)
}

// TestChaos_AuthPGDownRefreshTokenFailClosed stops Postgres and proves refresh rotation cannot succeed.
func TestChaos_AuthPGDownRefreshTokenFailClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAuthTestInfra(t)
	defer cleanup()

	svc := infra.newService(t)
	ctx := context.Background()
	email := "chaos-pg-refresh@example.com"
	password := "SuperSecure123!"

	_, _, refreshToken := infra.registerAndLogin(t, svc, email, password)

	stopAuthContainer(t, infra.PGContainer)
	requireAuthFaultActive(t, func() bool {
		return infra.Pool.Ping(ctx) != nil
	}, "pg ping must fail after stop")

	denied := 0
	const attempts = 8
	for i := 0; i < attempts; i++ {
		_, _, err := svc.RefreshToken(ctx, refreshToken, time.Hour)
		if err != nil {
			denied++
		}
	}

	logChaosProof(t, "postgres_stop_refresh", map[string]string{
		"subsystem":    "auth",
		"op":           "refresh_token",
		"baseline_ok":  "true",
		"denied":       strconv.Itoa(denied) + "/" + strconv.Itoa(attempts),
		"fault_verify": "postgres_container_stopped",
	})
	require.Equal(t, attempts, denied,
		"RefreshToken must fail when Postgres is down, got %d/%d", denied, attempts)
}

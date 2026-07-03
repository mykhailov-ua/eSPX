package management

import (
	"context"
	"strconv"
	"testing"
	"time"

	"espx/internal/config"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestChaos_MgmtRedisTerminateOutboxStaysPending kills Redis and proves outbox events revert to PENDING.
func TestChaos_MgmtRedisTerminateOutboxStaysPending(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupMgmtChaosInfra(t)
	defer cleanup()

	cfg := &config.Config{CampaignUpdateChannel: "campaigns:mgmt-redis-kill"}
	svc := newBareService(t, infra.Pool, []redis.UniversalClient{infra.Redis}, cfg)
	ctx := context.Background()

	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"rate_limit_per_min": "77"}))
	eventID := latestOutboxEventID(t, infra.Pool, "UPDATE_SETTINGS")

	worker := NewOutboxWorker(svc)
	require.NoError(t, infra.RedisContainer.Terminate(ctx))
	requireMgmtFaultActive(t, func() bool {
		return infra.Redis.Ping(ctx).Err() != nil
	}, "redis ping must fail after terminate")

	processed, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, processed)

	status := outboxStatus(t, infra.Pool, eventID)
	require.Equal(t, "PENDING", status)

	logChaosProof(t, "redis_container_terminate", map[string]string{
		"subsystem":    "management_outbox",
		"op":           "process_outbox",
		"baseline_ok":  "true",
		"status":       status,
		"processed":    "0",
		"fault_verify": "redis_container_terminated",
	})
}

// TestChaos_MgmtRedisStopStartOutboxRecovery stops Redis, proves PENDING, then drains after restart.
func TestChaos_MgmtRedisStopStartOutboxRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupMgmtChaosInfra(t)
	defer cleanup()

	cfg := &config.Config{CampaignUpdateChannel: "campaigns:mgmt-redis-recovery"}
	svc := newBareService(t, infra.Pool, []redis.UniversalClient{infra.Redis}, cfg)
	ctx := context.Background()

	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"rate_limit_per_min": "88"}))
	eventID := latestOutboxEventID(t, infra.Pool, "UPDATE_SETTINGS")

	worker := NewOutboxWorker(svc)

	stopMgmtContainer(t, infra.RedisContainer)
	requireMgmtFaultActive(t, func() bool {
		return infra.Redis.Ping(ctx).Err() != nil
	}, "redis ping must fail after stop")

	processed, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, processed)
	require.Equal(t, "PENDING", outboxStatus(t, infra.Pool, eventID))

	startMgmtContainer(t, infra.RedisContainer)
	infra.refreshRedisClient(t)
	rebindBareService(svc, infra)

	recovered := false
	require.Eventually(t, func() bool {
		n, err := worker.ProcessOutboxWithCount(ctx, 10)
		if err != nil || n != 1 {
			return false
		}
		recovered = outboxStatus(t, infra.Pool, eventID) == "PROCESSED"
		return recovered
	}, 30*time.Second, 200*time.Millisecond, "outbox must drain after Redis restart")

	val, err := infra.Redis.HGet(ctx, "config:values", "rate_limit_per_min").Result()
	require.NoError(t, err)
	require.Equal(t, "88", val)

	logChaosProof(t, "redis_stop_start_recovery", map[string]string{
		"subsystem":    "management_outbox",
		"op":           "process_outbox",
		"baseline_ok":  "true",
		"recovered":    strconv.FormatBool(recovered),
		"fault_verify": "redis_container_stopped_then_started",
	})
}

// TestChaos_MgmtPGStopOutboxClaimBlocked stops Postgres and proves outbox claim cannot proceed.
func TestChaos_MgmtPGStopOutboxClaimBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupMgmtChaosInfra(t)
	defer cleanup()

	cfg := &config.Config{CampaignUpdateChannel: "campaigns:mgmt-pg-stop"}
	svc := newBareService(t, infra.Pool, []redis.UniversalClient{infra.Redis}, cfg)
	ctx := context.Background()

	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"emergency_breaker": "false"}))
	eventID := latestOutboxEventID(t, infra.Pool, "UPDATE_SETTINGS")

	worker := NewOutboxWorker(svc)

	stopMgmtContainer(t, infra.PGContainer)
	requireMgmtFaultActive(t, func() bool {
		return infra.Pool.Ping(ctx) != nil
	}, "pg ping must fail after stop")

	processed, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.Error(t, err)
	require.Equal(t, 0, processed)

	startMgmtContainer(t, infra.PGContainer)
	infra.refreshPGPool(t)
	rebindBareService(svc, infra)
	require.Equal(t, "PENDING", outboxStatus(t, infra.Pool, eventID))

	logChaosProof(t, "postgres_container_stop", map[string]string{
		"subsystem":    "management_outbox",
		"op":           "process_outbox",
		"baseline_ok":  "true",
		"status":       "PENDING",
		"processed":    "0",
		"fault_verify": "postgres_container_stopped",
	})
}

// TestChaos_MgmtPGStopStartOutboxRecovery stops Postgres, then drains outbox after pool refresh.
func TestChaos_MgmtPGStopStartOutboxRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupMgmtChaosInfra(t)
	defer cleanup()

	cfg := &config.Config{CampaignUpdateChannel: "campaigns:mgmt-pg-recovery"}
	svc := newBareService(t, infra.Pool, []redis.UniversalClient{infra.Redis}, cfg)
	ctx := context.Background()

	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"emergency_breaker": "true"}))
	eventID := latestOutboxEventID(t, infra.Pool, "UPDATE_SETTINGS")

	worker := NewOutboxWorker(svc)

	stopMgmtContainer(t, infra.PGContainer)
	requireMgmtFaultActive(t, func() bool {
		return infra.Pool.Ping(ctx) != nil
	}, "pg ping must fail after stop")

	startMgmtContainer(t, infra.PGContainer)
	infra.refreshPGPool(t)
	rebindBareService(svc, infra)

	recovered := false
	require.Eventually(t, func() bool {
		n, err := worker.ProcessOutboxWithCount(ctx, 10)
		if err != nil || n != 1 {
			return false
		}
		recovered = outboxStatus(t, infra.Pool, eventID) == "PROCESSED"
		return recovered
	}, 30*time.Second, 200*time.Millisecond, "outbox must drain after Postgres restart")

	val, err := infra.Redis.HGet(ctx, "config:values", "emergency_breaker").Result()
	require.NoError(t, err)
	require.Equal(t, "true", val)

	logChaosProof(t, "postgres_stop_start_recovery", map[string]string{
		"subsystem":    "management_outbox",
		"op":           "process_outbox",
		"baseline_ok":  "true",
		"recovered":    strconv.FormatBool(recovered),
		"fault_verify": "postgres_container_stopped_then_started",
	})
}

package ingest

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"espx/internal/ads/sharding"
	adstest "espx/internal/ads/testutil"
	"espx/internal/config"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// Postgres health double for partial failure health checks.
type mockPinger struct {
	fail bool
}

func (m *mockPinger) Ping(ctx context.Context) error {
	if m.fail {
		return errors.New("ping failed")
	}
	return nil
}

// Redis client that simulates shard ping success or failure.
type mockFailRedis struct {
	redis.UniversalClient
	fail bool
}

func (m *mockFailRedis) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	if m.fail {
		cmd.SetErr(errors.New("redis connection refused"))
	} else {
		cmd.SetVal("PONG")
	}
	return cmd
}

// Guards gnet GET /health reports OK or DEGRADED from probe atomics (production path).
func TestHealthCheckPartialFailure(t *testing.T) {
	cfg := &config.Config{}
	registry := &adstest.MockRegistry{}

	t.Run("All Healthy", func(t *testing.T) {
		rdbs := []redis.UniversalClient{&mockFailRedis{fail: false}}
		pool := &mockPinger{fail: false}
		sharder := sharding.NewJumpHashSharder(1)
		handler := NewAdsPacketHandler(cfg, registry, nil, pool, rdbs, sharder, "fraud-stream", nil)
		handler.SetHealthProbeState(true, true)

		status, body := GetHealthGnet(handler)
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, body, "OK")
	})

	t.Run("Postgres Down", func(t *testing.T) {
		rdbs := []redis.UniversalClient{&mockFailRedis{fail: false}}
		pool := &mockPinger{fail: true}
		sharder := sharding.NewJumpHashSharder(1)
		handler := NewAdsPacketHandler(cfg, registry, nil, pool, rdbs, sharder, "fraud-stream", nil)
		handler.SetHealthProbeState(false, true)

		status, body := GetHealthGnet(handler)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, body, "DEGRADED")
	})

	t.Run("Redis Shard 2 Down", func(t *testing.T) {
		rdbs := []redis.UniversalClient{
			&mockFailRedis{fail: false},
			&mockFailRedis{fail: true},
		}
		pool := &mockPinger{fail: false}
		sharder := sharding.NewJumpHashSharder(1)
		handler := NewAdsPacketHandler(cfg, registry, nil, pool, rdbs, sharder, "fraud-stream", nil)
		handler.SetHealthProbeState(false, true, false)

		status, body := GetHealthGnet(handler)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, body, "DEGRADED")
		assert.Contains(t, body, "redis=")
	})
}

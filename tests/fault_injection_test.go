package tests

import (
	"context"
	"net/http"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_RedisTerminateStopsIngest kills the live Redis container and proves /track stops accepting.
func TestChaos_RedisTerminateStopsIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupChaosInfra(t)
	defer cleanup()

	stack := startIngestStack(t, infra.Pool, infra.Redis, "chaos-redis-kill")
	defer stack.Close(t)

	const preFault = 5
	for i := 0; i < preFault; i++ {
		require.Equal(t, http.StatusAccepted, postClick(t, stack.Srv.URL, stack.CampaignID))
	}

	require.Eventually(t, func() bool {
		return countCampaignEvents(t, infra.Pool, stack.CampaignID) >= int64(preFault)
	}, 10*time.Second, 100*time.Millisecond)

	ctx := context.Background()
	require.NoError(t, infra.RedisContainer.Terminate(ctx))
	time.Sleep(200 * time.Millisecond)

	postFaultFail := 0
	const postFaultAttempts = 10
	for i := 0; i < postFaultAttempts; i++ {
		code := postClick(t, stack.Srv.URL, stack.CampaignID)
		if code != http.StatusAccepted {
			postFaultFail++
		}
	}

	t.Logf("chaos_proof fault=redis_container_terminate pre_accept_rate=1.00 post_non_202=%d/%d",
		postFaultFail, postFaultAttempts)
	logChaosProof(t, "redis_container_terminate", map[string]string{
		"subsystem":    "ingest",
		"baseline_ok":  "true",
		"post_non_202": itoaChaos(postFaultFail) + "/" + itoaChaos(postFaultAttempts),
		"fault_verify": "redis_container_terminated",
	})
	require.GreaterOrEqual(t, postFaultFail, 8,
		"after Redis terminate, ingest must fail (expected 503/unavailable), got only %d failures", postFaultFail)
}

// TestChaos_PostgresKillOpensConsumerCircuit terminates Postgres and proves the consumer circuit opens.
func TestChaos_PostgresKillOpensConsumerCircuit(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupChaosInfra(t)
	defer cleanup()

	stack := startIngestStack(t, infra.Pool, infra.Redis, "chaos-pg-kill")
	defer stack.Close(t)

	producer := ads.NewStreamProducer(infra.Redis, stack.Stream, 1000, 1*time.Second)
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		require.NoError(t, producer.Process(domainEventClick(stack.CampaignID)))
	}

	require.Eventually(t, func() bool {
		return countCampaignEvents(t, infra.Pool, stack.CampaignID) >= 3
	}, 10*time.Second, 100*time.Millisecond)

	rowsBefore := countCampaignEvents(t, infra.Pool, stack.CampaignID)
	require.NoError(t, infra.PGContainer.Terminate(ctx))
	time.Sleep(300 * time.Millisecond)
	require.Error(t, infra.Pool.Ping(ctx), "chaos_proof: pg ping must fail after container terminate")

	for i := 0; i < 20; i++ {
		_ = producer.Process(domainEventClick(stack.CampaignID))
	}

	require.Eventually(t, func() bool {
		return stack.Consumer.CircuitBreakerState() == ads.CircuitOpen
	}, 20*time.Second, 100*time.Millisecond, "consumer circuit must open when PG is dead")

	streamLen, err := infra.Redis.XLen(ctx, stack.Stream).Result()
	require.NoError(t, err)

	t.Logf("chaos_proof fault=postgres_container_terminate pg_ping_failed=true circuit=%s rows_before_fault=%d stream_len=%d",
		stack.Consumer.CircuitBreakerState(), rowsBefore, streamLen)
	logChaosProof(t, "postgres_container_terminate", map[string]string{
		"subsystem":    "ingest_consumer",
		"baseline_ok":  "true",
		"pg_ping":      "failed",
		"circuit":      stack.Consumer.CircuitBreakerState().String(),
		"rows_before":  itoaChaos(int(rowsBefore)),
		"stream_len":   itoaChaos(int(streamLen)),
		"fault_verify": "postgres_container_terminated",
	})
	assert.Greater(t, streamLen, int64(0), "stream must retain messages while PG is down")
	assert.Equal(t, ads.CircuitOpen, stack.Consumer.CircuitBreakerState())
}

// TestChaos_StreamBacklogUnderPostgresOutage accepts events to Redis while PG is down; stream must grow.
func TestChaos_StreamBacklogUnderPostgresOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupChaosInfra(t)
	defer cleanup()

	stack := startIngestStack(t, infra.Pool, infra.Redis, "chaos-pg-backlog")
	defer stack.Close(t)

	const seeded = 4
	for i := 0; i < seeded; i++ {
		require.Equal(t, http.StatusAccepted, postClick(t, stack.Srv.URL, stack.CampaignID))
	}
	require.Eventually(t, func() bool {
		return countCampaignEvents(t, infra.Pool, stack.CampaignID) >= seeded
	}, 10*time.Second, 100*time.Millisecond)

	rowsBefore := countCampaignEvents(t, infra.Pool, stack.CampaignID)
	ctx := context.Background()
	require.NoError(t, infra.PGContainer.Terminate(ctx))
	time.Sleep(300 * time.Millisecond)

	streamBefore, err := infra.Redis.XLen(ctx, stack.Stream).Result()
	require.NoError(t, err)

	postFaultAccepted := 0
	const attempts = 6
	for i := 0; i < attempts; i++ {
		if postClick(t, stack.Srv.URL, stack.CampaignID) == http.StatusAccepted {
			postFaultAccepted++
		}
	}

	streamAfter, err := infra.Redis.XLen(ctx, stack.Stream).Result()
	require.NoError(t, err)

	t.Logf("chaos_proof fault=postgres_container_terminate rows_before=%d stream_delta=%d post_fault_accepted=%d/%d",
		rowsBefore, streamAfter-streamBefore, postFaultAccepted, attempts)
	logChaosProof(t, "postgres_container_terminate", map[string]string{
		"subsystem":           "ingest_track",
		"baseline_ok":         "true",
		"rows_before":         itoaChaos(int(rowsBefore)),
		"stream_delta":        itoaChaos(int(streamAfter - streamBefore)),
		"post_fault_accepted": itoaChaos(postFaultAccepted) + "/" + itoaChaos(attempts),
		"fault_verify":        "postgres_container_terminated",
	})
	assert.GreaterOrEqual(t, streamAfter, streamBefore+int64(postFaultAccepted),
		"accepted events must land in Redis stream while PG is down")
}

func domainEventClick(campaignID uuid.UUID) *domain.Event {
	return &domain.Event{
		CampaignID: campaignID,
		Type:       "click",
		ClickID:    uuid.NewString(),
	}
}

func itoaChaos(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

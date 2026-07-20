package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards fraud blacklist adds publish fraud:quarantine for immediate edge flush.
func TestApplyBlacklistPayload_publishesQuarantine(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	pubsub := rdb.Subscribe(ctx, fraudQuarantineChannel)
	defer pubsub.Close()
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	svc := &Service{rdbs: []redis.UniversalClient{rdb}}
	worker := &OutboxWorker{svc: svc}

	require.NoError(t, worker.applyBlacklistPayload(ctx, BlacklistPayload{
		Action: "add",
		IP:     "203.0.113.10",
		Reason: "fraud",
	}, time.Now()))

	msg, err := pubsub.ReceiveTimeout(ctx, 3*time.Second)
	require.NoError(t, err)
	payload, ok := msg.(*redis.Message)
	require.True(t, ok)
	assert.Equal(t, fraudQuarantineChannel, payload.Channel)
	assert.Equal(t, "203.0.113.10", payload.Payload)

	isMember, err := rdb.SIsMember(ctx, "blacklist:fraud", "203.0.113.10").Result()
	require.NoError(t, err)
	assert.True(t, isMember)
}

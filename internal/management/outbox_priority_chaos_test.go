package management

import (
	"context"
	"encoding/json"
	"testing"

	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_OutboxPriorityLanes guards high-priority outbox events are claimed before bulk backlog.
func TestChaos_OutboxPriorityLanes(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{CampaignUpdateChannel: "campaigns:update-priority"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	ctx := context.Background()

	const pacingBacklog = 500
	pacingPayload, err := json.Marshal(campaignPacingPayload{
		CampaignID: uuid.New().String(),
		PacingMode: "even",
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO outbox_events (event_type, payload)
		SELECT 'UPDATE_CAMPAIGN_PACING', $1::jsonb
		FROM generate_series(1, $2)`, pacingPayload, pacingBacklog)
	require.NoError(t, err)

	blPayload, err := json.Marshal(BlacklistPayload{
		Action: "add",
		IP:     "203.0.113.99",
		Reason: "manual",
	})
	require.NoError(t, err)

	var blacklistID int64
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO outbox_events (event_type, payload) VALUES ('UPDATE_BLACKLIST', $1) RETURNING id`,
		blPayload,
	).Scan(&blacklistID))

	worker := NewOutboxWorker(svc)
	processed, err := worker.ProcessOutboxWithCount(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	var blacklistStatus string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT status FROM outbox_events WHERE id = $1`, blacklistID,
	).Scan(&blacklistStatus))
	assert.Equal(t, "PROCESSED", blacklistStatus)

	var pendingPacing int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox_events
		WHERE event_type = 'UPDATE_CAMPAIGN_PACING' AND status = 'PENDING'`,
	).Scan(&pendingPacing))
	assert.Equal(t, pacingBacklog, pendingPacing)

	ok, err := rdb.SIsMember(ctx, "blacklist:manual", "203.0.113.99").Result()
	require.NoError(t, err)
	assert.True(t, ok, "blacklist side effect must land in Redis before pacing backlog drains")

	logChaosProof(t, "outbox_priority_lanes", map[string]string{
		"subsystem":       "management_outbox",
		"pacing_backlog":  "500",
		"blacklist_first": "true",
		"baseline_ok":     "true",
		"fault_type":      "priority_inversion",
	})
}

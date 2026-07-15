package management

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPubSubShards = 3

// campaignIDForShard returns a UUID that StaticSlotSharder maps to wantShard.
func campaignIDForShard(t *testing.T, numShards, wantShard int) uuid.UUID {
	t.Helper()
	sharder := ingestion.NewStaticSlotSharder(numShards)
	for range 20_000 {
		id := uuid.New()
		if sharder.GetShard(id) == wantShard {
			return id
		}
	}
	t.Fatalf("could not find campaign id for shard %d within %d shards", wantShard, numShards)
	return uuid.Nil
}

// newDedicatedRedisShards starts one Redis container per logical shard so pub/sub is isolated.
func newDedicatedRedisShards(t *testing.T, n int) []redis.UniversalClient {
	t.Helper()
	shards := make([]redis.UniversalClient, n)
	for i := range shards {
		rdb, cleanup := database.SetupTestRedis(t)
		t.Cleanup(cleanup)
		shards[i] = rdb
	}
	return shards
}

func newIsolatedRedisShards(t *testing.T) []redis.UniversalClient {
	t.Helper()
	rdb0, cleanupRedis := database.SetupTestRedis(t)
	t.Cleanup(cleanupRedis)

	var endpoint string
	switch client := rdb0.(type) {
	case *redis.Client:
		endpoint = client.Options().Addr
	default:
		t.Fatalf("unexpected redis client type %T", rdb0)
	}

	shards := make([]redis.UniversalClient, testPubSubShards)
	shards[0] = rdb0
	for i := 1; i < testPubSubShards; i++ {
		rdb := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: []string{endpoint},
			DB:    i,
		})
		require.NoError(t, rdb.Ping(context.Background()).Err())
		t.Cleanup(func() { _ = rdb.Close() })
		shards[i] = rdb
	}
	return shards
}

// TestPublishCampaignUpdate_RoutesToShardZero guards pub/sub always uses shard 0.
func TestPublishCampaignUpdate_RoutesToShardZero(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-container redis test")
	}

	shards := newDedicatedRedisShards(t, testPubSubShards)
	svc := &Service{rdbs: shards, cfg: &config.Config{CampaignUpdateChannel: "test:pubsub:shard0"}}
	ctx := context.Background()
	channel := svc.campaignUpdateChannel()

	sub0 := shards[0].Subscribe(ctx, channel)
	defer sub0.Close()
	sub2 := shards[2].Subscribe(ctx, channel)
	defer sub2.Close()

	campaignID := uuid.New().String()
	require.NoError(t, svc.publishCampaignUpdate(ctx, campaignID))

	msg, err := sub0.ReceiveMessage(ctx)
	require.NoError(t, err)
	assert.Equal(t, campaignID, msg.Payload)

	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err = sub2.ReceiveMessage(waitCtx)
	assert.Error(t, err, "pub/sub must not reach non-zero redis instances")

	assert.Same(t, shards[0], svc.getPubSubRDB())
}

// TestOutboxScheduleUpdate_PubSubOnShardZero guards schedule notifications publish on shard 0 only.
func TestOutboxScheduleUpdate_PubSubOnShardZero(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-container redis test")
	}

	shards := newDedicatedRedisShards(t, testPubSubShards)
	campaignID := campaignIDForShard(t, testPubSubShards, 2)
	require.Equal(t, 2, ingestion.NewStaticSlotSharder(testPubSubShards).GetShard(campaignID))

	channel := "test:schedule:pubsub"
	svc := &Service{
		rdbs:    shards,
		sharder: ingestion.NewStaticSlotSharder(testPubSubShards),
		cfg:     &config.Config{CampaignUpdateChannel: channel},
	}

	ctx := context.Background()
	sub0 := shards[0].Subscribe(ctx, channel)
	defer sub0.Close()
	subCampaignShard := shards[2].Subscribe(ctx, channel)
	defer subCampaignShard.Close()

	worker := NewOutboxWorker(svc)
	payload, err := json.Marshal(map[string]string{"campaign_id": campaignID.String()})
	require.NoError(t, err)
	require.NoError(t, worker.handleUpdateCampaignSchedule(ctx, payload))

	msg, err := sub0.ReceiveMessage(ctx)
	require.NoError(t, err)
	assert.Equal(t, campaignID.String(), msg.Payload)

	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err = subCampaignShard.ReceiveMessage(waitCtx)
	assert.Error(t, err, "pub/sub must not publish on the campaign data shard redis instance")
}

// TestOutboxCreateCampaign_BudgetOnCampaignShard verifies budget keys stay on the campaign shard.
func TestOutboxCreateCampaign_BudgetOnCampaignShard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	shards := newIsolatedRedisShards(t)
	campaignID := campaignIDForShard(t, testPubSubShards, 1)
	channel := "test:create:pubsub"
	svc := newBareService(t, pool, shards, &config.Config{CampaignUpdateChannel: channel})
	ctx := context.Background()

	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "PubSub Customer", 500_000_000, "USD"))

	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'shard test', 100000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)
	`, ingestion.ToUUID(campaignID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	sub0 := shards[0].Subscribe(ctx, channel)
	defer sub0.Close()

	worker := NewOutboxWorker(svc)
	payload, err := json.Marshal(CampaignPayload{
		CampaignID:  campaignID.String(),
		BudgetLimit: 100_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, worker.handleCreateCampaign(ctx, payload))

	budgetKey := "budget:campaign:" + campaignID.String()
	exists, err := shards[1].Exists(ctx, budgetKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "budget key must live on campaign shard")

	exists, err = shards[0].Exists(ctx, budgetKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "budget key must not be written to pubsub shard")

	receiveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	msg, err := sub0.ReceiveMessage(receiveCtx)
	require.NoError(t, err)
	assert.Equal(t, campaignID.String(), msg.Payload)
}

// TestGetPubSubRDB_ReturnsFirstShard is a fast unit check without containers.
func TestGetPubSubRDB_ReturnsFirstShard(t *testing.T) {
	svc := &Service{rdbs: []redis.UniversalClient{nil, nil, nil}}
	assert.Nil(t, svc.getPubSubRDB())

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:9"})
	svc.rdbs = []redis.UniversalClient{rdb, redis.NewClient(&redis.Options{Addr: "127.0.0.1:8"})}
	assert.Same(t, rdb, svc.getPubSubRDB())
}

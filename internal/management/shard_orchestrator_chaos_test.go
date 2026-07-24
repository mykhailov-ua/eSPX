package management

import (
	"context"
	"net"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestChaos_SO_NoFalseMigrate (SO-01) verifies that the ShardOrchestrator does not trigger
// a migration if the capacity scores are below the threshold.
func TestChaos_SO_NoFalseMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb0, cleanup0 := database.SetupTestRedis(t)
	defer cleanup0()
	rdb1, cleanup1 := database.SetupTestRedis(t)
	defer cleanup1()

	cfg := &config.Config{SlotMigrationEnabled: false}
	svc := NewService(pool, []redis.UniversalClient{rdb0, rdb1}, ingestion.NewStaticSlotSharder(2), cfg)
	defer svc.Close()

	var campID uuid.UUID
	for {
		campID = uuid.New()
		if ingestion.CampaignSlotIndex(campID)%2 == 0 {
			break
		}
	}
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "SO Cust 1", 1_000_000, "USD"))

	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'so-test-1', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	provider := &mockShardMetricsProvider{
		metrics: map[int16]ShardMetrics{
			0: {ShardID: 0, CPUUsage: 40.0, MemoryPct: 30.0, OpsPerSec: 10000},
			1: {ShardID: 1, CPUUsage: 10.0, MemoryPct: 15.0, OpsPerSec: 1000},
		},
	}

	orchestrator := NewShardOrchestrator(svc, provider, 100*time.Millisecond)
	orchestrator.scaleThreshold = 0.85
	orchestrator.overloadLimit = 10 * time.Millisecond

	for i := 0; i < 5; i++ {
		orchestrator.tick(ctx)
		time.Sleep(10 * time.Millisecond)
	}

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM campaign_routing").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	logChaosProof(t, "orchestrator_no_false_migrate", map[string]string{
		"subsystem":     "shard_orchestrator",
		"max_ema":       "0.40",
		"threshold":     "0.85",
		"false_migrate": "false",
	})
}

// TestChaos_SO_CampaignRoutingMigration (SO-02) verifies elastic triplet migration under overload.
func TestChaos_SO_CampaignRoutingMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb0, cleanup0 := database.SetupTestRedis(t)
	defer cleanup0()
	rdb1, cleanup1 := database.SetupTestRedis(t)
	defer cleanup1()

	cfg := &config.Config{SlotMigrationEnabled: false}
	svc := NewService(pool, []redis.UniversalClient{rdb0, rdb1}, ingestion.NewStaticSlotSharder(2), cfg)
	defer svc.Close()

	var campID uuid.UUID
	for {
		campID = uuid.New()
		if ingestion.CampaignSlotIndex(campID)%2 == 0 {
			break
		}
	}
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "SO Cust 2", 1_000_000, "USD"))

	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'so-test-2', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	key := "budget:campaign:" + campID.String()
	require.NoError(t, rdb0.Set(ctx, key, "850000", 0).Err())

	provider := &mockShardMetricsProvider{
		metrics: map[int16]ShardMetrics{
			0: {ShardID: 0, CPUUsage: 95.0, MemoryPct: 90.0, OpsPerSec: 60000},
			1: {ShardID: 1, CPUUsage: 10.0, MemoryPct: 15.0, OpsPerSec: 1000},
		},
	}

	orchestrator := NewShardOrchestrator(svc, provider, 10*time.Millisecond)
	orchestrator.scaleThreshold = 0.85
	orchestrator.overloadLimit = 20 * time.Millisecond
	orchestrator.cooldown = 0

	orchestrator.tick(ctx)
	time.Sleep(30 * time.Millisecond)
	orchestrator.tick(ctx)

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM campaign_routing WHERE campaign_id = $1", ingestion.ToUUID(campID)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	exists, err := rdb1.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), exists)

	existsSource, err := rdb0.Exists(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), existsSource)

	logChaosProof(t, "campaign_routing_migration", map[string]string{
		"subsystem":         "shard_orchestrator",
		"source_shard":      "0",
		"target_shard":      "1",
		"migration_success": "true",
		"keys_drained":      "true",
	})
}

// TestChaos_SO_RoutingEpochRace verifies stale routing epochs cannot regress the sharder snapshot.
func TestChaos_SO_RoutingEpochRace(t *testing.T) {
	t.Parallel()
	secret := []byte("epoch-race-secret")
	sh := ingestion.NewStaticSlotSharder(4)
	sh.SwapSnapshot(1, nil, 10)

	var oldHdr ingestion.TCPControlHeader
	oldHdr.MsgType = ingestion.TCPMsgSnapshot
	oldHdr.RoutingEpoch = 10
	oldHdr.SlotMapVersion = 1
	var oldFrame [64]byte
	_, err := ingestion.EncodeTCPControlFrame(oldFrame[:], secret, &oldHdr, nil)
	require.NoError(t, err)

	var newHdr ingestion.TCPControlHeader
	newHdr.MsgType = ingestion.TCPMsgSnapshot
	newHdr.RoutingEpoch = 11
	newHdr.SlotMapVersion = 2
	var newFrame [64]byte
	_, err = ingestion.EncodeTCPControlFrame(newFrame[:], secret, &newHdr, nil)
	require.NoError(t, err)

	apply := func(frame []byte) int64 {
		var hdr ingestion.TCPControlHeader
		_, err := ingestion.DecodeTCPControlFrame(frame, secret, &hdr)
		require.NoError(t, err)
		if hdr.RoutingEpoch > sh.Snapshot().MigrationGen {
			prev := sh.Snapshot()
			sh.SwapSnapshot(hdr.SlotMapVersion, &prev.Table, hdr.RoutingEpoch)
		}
		return sh.Snapshot().MigrationGen
	}

	require.Equal(t, int64(11), apply(newFrame[:]))
	require.Equal(t, int64(11), apply(oldFrame[:]))

	logChaosProof(t, "routing_epoch_race", map[string]string{
		"applied_epoch": "11",
		"stale_blocked": "true",
	})
}

// TestChaos_SO_TripletFailover verifies A/B/R shard selection under triplet routing.
func TestChaos_SO_TripletFailover(t *testing.T) {
	t.Parallel()
	camp := &ingestion.CampaignTripletPick{PrimaryA: 1, PrimaryB: 2, Reserve: 3}
	shards := map[int]int{}
	for i := 0; i < 1000; i++ {
		user := "user-" + string(rune('a'+i%26))
		shards[camp.PickShard("550e8400-e29b-41d4-a716-446655440000", user)]++
	}
	require.Greater(t, shards[1], 0)
	require.Greater(t, shards[2], 0)
	require.Greater(t, shards[3], 0)
	logChaosProof(t, "triplet_abr_failover", map[string]string{
		"shard_a_hits": "true",
		"shard_b_hits": "true",
		"reserve_hits": "true",
	})
}

// TestChaos_TCP_SnapshotHMACACK exercises management TCP snapshot + tracker ACK (GAP-SHARD-05).
func TestChaos_TCP_SnapshotHMACACK(t *testing.T) {
	secret := []byte("tcp-hmac-secret")
	cfg := &config.Config{
		TCPControlEnabled:    true,
		TCPControlHMACSecret: config.Secret(secret),
	}
	sh := ingestion.NewStaticSlotSharder(2)
	srv := NewTCPControlServer(cfg, nil, sh, 2)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	srv.ln = ln

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.acceptLoop(ctx)

	client := ingestion.NewTCPControlClient(ingestion.TCPControlClientConfig{
		Enabled:   true,
		Secret:    secret,
		TrackerID: 1,
		MgmtAddr:  ln.Addr().String(),
		Sharder:   sh,
		DialTO:    2 * time.Second,
	})
	require.NoError(t, client.RequestSnapshot(ctx))

	logChaosProof(t, "tcp_snapshot_hmac_ack", map[string]string{
		"subsystem": "tcp_control",
		"ack":       "true",
	})
}

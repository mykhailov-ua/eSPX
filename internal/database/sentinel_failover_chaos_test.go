package database

import (
	"context"
	"hash/crc32"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

const (
	sentinelChaosShards          = 4
	sentinelBudgetPreRemaining   = int64(900_000)
	sentinelBudgetPreSyncDelta   = int64(5_000)
	sentinelChaosMarkerKey       = "sentinel:chaos:marker"
	sentinelChaosBudgetPreRemKey = "sentinel:chaos:budget:pre_remaining"
	sentinelChaosBudgetPreSyncKey = "sentinel:chaos:budget:pre_sync"
	sentinelChaosBudgetCampaignKey = "sentinel:chaos:budget:campaign_id"
	sentinelChaosLoadShard0ErrKey = "sentinel:chaos:load:shard0:errors"
	sentinelChaosLoadShard0OKKey  = "sentinel:chaos:load:shard0:ok"
	sentinelChaosLoadOtherOKKey   = "sentinel:chaos:load:other:ok"
)

func logSentinelChaosProof(t *testing.T, fault string, kv map[string]string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("chaos_proof fault=")
	b.WriteString(fault)
	for k, v := range kv {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
	}
	t.Log(b.String())
}

func campaignShard(id uuid.UUID, numShards int) int {
	table := crc32.MakeTable(crc32.Castagnoli)
	key := crc32.Checksum(id[:], table)
	slot := key & 1023
	return int(slot % uint32(numShards))
}

func campaignIDForShard(t *testing.T, numShards, wantShard int) uuid.UUID {
	t.Helper()
	for range 20_000 {
		id := uuid.New()
		if campaignShard(id, numShards) == wantShard {
			return id
		}
	}
	t.Fatalf("could not find campaign id for shard %d", wantShard)
	return uuid.Nil
}

func budgetCampaignKey(id uuid.UUID) string {
	return "budget:campaign:" + id.String()
}

func budgetSyncKey(id uuid.UUID) string {
	return "budget:sync:campaign:" + id.String()
}

// TestSentinelFailoverLoadWorker hammers all shards via Sentinel while the orchestrator pauses redis-0.
// Run with SENTINEL_LOAD_WORKER=1 in background; stop with SIGTERM after promotion.
func TestSentinelFailoverLoadWorker(t *testing.T) {
	if os.Getenv("SENTINEL_LOAD_WORKER") != "1" {
		t.Skip("orchestrator must set SENTINEL_LOAD_WORKER=1 and run this test in background during failover")
	}
	cfg := sentinelChaosConfig(t)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	clients, err := ConnectRedisShards(dialCtx, cfg, RedisShardOptions{PoolSize: 8, FilterTimeoutMs: 100})
	if err != nil {
		t.Fatalf("ConnectRedisShards: %v", err)
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	campaignIDs := make([]uuid.UUID, sentinelChaosShards)
	for i := range campaignIDs {
		campaignIDs[i] = campaignIDForShard(t, sentinelChaosShards, i)
	}

	seedCtx, seedCancel := context.WithTimeout(ctx, 30*time.Second)
	defer seedCancel()
	if err := seedShard0Budget(seedCtx, clients[0], campaignIDs[0]); err != nil {
		t.Fatalf("seed shard 0 budget: %v", err)
	}
	for i := 1; i < len(clients); i++ {
		if err := clients[i].Set(seedCtx, budgetCampaignKey(campaignIDs[i]), sentinelBudgetPreRemaining, 0).Err(); err != nil {
			t.Fatalf("seed shard %d budget: %v", i, err)
		}
	}
	if err := clients[0].Set(seedCtx, sentinelChaosMarkerKey, "ok", 0).Err(); err != nil {
		t.Fatalf("seed marker: %v", err)
	}
	_ = clients[1].Del(seedCtx, sentinelChaosLoadShard0ErrKey, sentinelChaosLoadShard0OKKey, sentinelChaosLoadOtherOKKey)

	var (
		shard0Errors atomic.Int64
		shard0OK     atomic.Int64
		otherOK      atomic.Int64
		panics       atomic.Int32
	)
	var wg sync.WaitGroup
	for shardIdx, rdb := range clients {
		shardIdx := shardIdx
		rdb := rdb
		campID := campaignIDs[shardIdx]
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					panics.Add(1)
				}
			}()
			loadShardLoop(ctx, rdb, shardIdx, campID, &shard0Errors, &shard0OK, &otherOK)
		}()
	}

	<-ctx.Done()
	wg.Wait()

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()
	if err := flushLoadStats(flushCtx, clients[1], &shard0Errors, &shard0OK, &otherOK); err != nil {
		t.Fatalf("flush load stats: %v", err)
	}
	if panics.Load() > 0 {
		t.Fatalf("load worker panicked %d times", panics.Load())
	}
}

// loadShardLoop issues track-like Redis reads (budget GET) until ctx is cancelled.
func loadShardLoop(ctx context.Context, rdb redis.UniversalClient, shardIdx int, campID uuid.UUID, shard0Errors, shard0OK, otherOK *atomic.Int64) {
	bKey := budgetCampaignKey(campID)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
			_, err := rdb.Get(opCtx, bKey).Result()
			cancel()
			recordLoadResult(shardIdx, err, shard0Errors, shard0OK, otherOK)
		}
	}
}

func recordLoadResult(shardIdx int, err error, shard0Errors, shard0OK, otherOK *atomic.Int64) {
	if err != nil {
		if shardIdx == 0 {
			shard0Errors.Add(1)
		}
		return
	}
	if shardIdx == 0 {
		shard0OK.Add(1)
	} else {
		otherOK.Add(1)
	}
}

func flushLoadStats(ctx context.Context, rdb redis.UniversalClient, shard0Errors, shard0OK, otherOK *atomic.Int64) error {
	pipe := rdb.Pipeline()
	pipe.Set(ctx, sentinelChaosLoadShard0ErrKey, shard0Errors.Load(), 0)
	pipe.Set(ctx, sentinelChaosLoadShard0OKKey, shard0OK.Load(), 0)
	pipe.Set(ctx, sentinelChaosLoadOtherOKKey, otherOK.Load(), 0)
	_, err := pipe.Exec(ctx)
	return err
}

func seedShard0Budget(ctx context.Context, rdb redis.UniversalClient, campID uuid.UUID) error {
	pipe := rdb.Pipeline()
	pipe.Set(ctx, budgetCampaignKey(campID), sentinelBudgetPreRemaining, 0)
	pipe.Set(ctx, budgetSyncKey(campID), sentinelBudgetPreSyncDelta, 0)
	pipe.Set(ctx, sentinelChaosBudgetPreRemKey, sentinelBudgetPreRemaining, 0)
	pipe.Set(ctx, sentinelChaosBudgetPreSyncKey, sentinelBudgetPreSyncDelta, 0)
	pipe.Set(ctx, sentinelChaosBudgetCampaignKey, campID.String(), 0)
	_, err := pipe.Exec(ctx)
	return err
}

// TestSentinelActiveFailoverVerify runs after redis-0 pause and replica promotion.
func TestSentinelActiveFailoverVerify(t *testing.T) {
	if os.Getenv("SENTINEL_FAILOVER_DONE") != "1" {
		t.Skip("orchestrator must pause redis-0 and set SENTINEL_FAILOVER_DONE=1")
	}
	cfg := sentinelChaosConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	durationMs, err := strconv.Atoi(os.Getenv("SENTINEL_FAILOVER_DURATION_MS"))
	if err != nil || durationMs < 0 {
		t.Fatalf("SENTINEL_FAILOVER_DURATION_MS: %q", os.Getenv("SENTINEL_FAILOVER_DURATION_MS"))
	}
	maxMs := 15_000
	if raw := os.Getenv("SENTINEL_FAILOVER_MAX_MS"); raw != "" {
		maxMs, err = strconv.Atoi(raw)
		if err != nil || maxMs <= 0 {
			t.Fatalf("SENTINEL_FAILOVER_MAX_MS: %q", raw)
		}
	}
	if durationMs > maxMs {
		t.Fatalf("failover duration %dms exceeds max %dms", durationMs, maxMs)
	}

	rdb, err := ConnectRedisShard(ctx, cfg, 0, RedisShardOptions{PoolSize: 4})
	if err != nil {
		t.Fatalf("ConnectRedisShard shard 0: %v", err)
	}
	defer func() { _ = rdb.Close() }()

	statsRdb, err := ConnectRedisShard(ctx, cfg, 1, RedisShardOptions{PoolSize: 4})
	if err != nil {
		t.Fatalf("ConnectRedisShard shard 1 (load stats): %v", err)
	}
	defer func() { _ = statsRdb.Close() }()

	val, err := rdb.Get(ctx, sentinelChaosMarkerKey).Result()
	if err != nil {
		t.Fatalf("GET %s after failover: %v", sentinelChaosMarkerKey, err)
	}
	if val != "ok" {
		t.Fatalf("GET %s = %q, want ok", sentinelChaosMarkerKey, val)
	}

	campRaw, err := rdb.Get(ctx, sentinelChaosBudgetCampaignKey).Result()
	if err != nil {
		t.Fatalf("GET %s: %v", sentinelChaosBudgetCampaignKey, err)
	}
	campID, err := uuid.Parse(campRaw)
	if err != nil {
		t.Fatalf("parse %s: %v", sentinelChaosBudgetCampaignKey, err)
	}
	preRemaining, err := rdb.Get(ctx, sentinelChaosBudgetPreRemKey).Int64()
	if err != nil {
		t.Fatalf("GET %s: %v", sentinelChaosBudgetPreRemKey, err)
	}
	preSync, err := rdb.Get(ctx, sentinelChaosBudgetPreSyncKey).Int64()
	if err != nil {
		t.Fatalf("GET %s: %v", sentinelChaosBudgetPreSyncKey, err)
	}

	postRemaining, err := rdb.Get(ctx, budgetCampaignKey(campID)).Int64()
	if err != nil {
		t.Fatalf("GET budget after failover: %v", err)
	}
	postSync, err := rdb.Get(ctx, budgetSyncKey(campID)).Int64()
	if err == redis.Nil {
		postSync = 0
	} else if err != nil {
		t.Fatalf("GET sync after failover: %v", err)
	}

	// CHAOS.md §3.1 on promoted master: remaining unchanged; sync delta preserved (±0 debits on hot path).
	if postRemaining != preRemaining {
		t.Fatalf("budget remaining after failover: got %d want %d (pre-failover)", postRemaining, preRemaining)
	}
	if postSync != preSync {
		t.Fatalf("budget sync delta after failover: got %d want %d", postSync, preSync)
	}

	shard0Errors, _ := statsRdb.Get(ctx, sentinelChaosLoadShard0ErrKey).Int64()
	shard0OK, _ := statsRdb.Get(ctx, sentinelChaosLoadShard0OKKey).Int64()
	otherOK, _ := statsRdb.Get(ctx, sentinelChaosLoadOtherOKKey).Int64()

	if shard0Errors == 0 {
		t.Fatalf("expected shard 0 load errors during failover window, got 0")
	}
	if otherOK == 0 {
		t.Fatalf("expected other shards to keep serving during failover, got other_ok=0")
	}
	if shard0OK == 0 {
		t.Log("warning: no shard0_ok during load (failover may have been too fast for successes)")
	}

	logSentinelChaosProof(t, "sentinel_active_failover", map[string]string{
		"duration_ms":       strconv.Itoa(durationMs),
		"budget_consistent": "true",
		"shard0_errors":     strconv.FormatInt(shard0Errors, 10),
		"shard0_ok":         strconv.FormatInt(shard0OK, 10),
		"other_ok":          strconv.FormatInt(otherOK, 10),
	})
}

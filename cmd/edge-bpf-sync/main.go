// Command edge-bpf-sync mirrors Redis shard-0 deny and allow sets into pinned XDP LPM trie maps.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"espx/internal/edge"
	"espx/internal/edge/allowlist"
	"espx/internal/edge/blocklist"
	"espx/pkg/lifecycle"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/redis/go-redis/v9"
)

func main() {
	syncInterval := edge.EnvDuration("SYNC_INTERVAL", 5*time.Second)
	blocklistPath := edge.EnvOr("BPF_BLOCKLIST_MAP", blocklist.DefaultMapPath)
	allowlistPath := edge.EnvOr("BPF_ALLOWLIST_MAP", allowlist.DefaultMapPath)
	redisAddr := edge.FirstRedisAddr()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if redisAddr == "" {
		slog.Error("REDIS_ADDRS or REDIS_HOST/REDIS_PORT must be set")
		os.Exit(1)
	}

	if err := rlimit.RemoveMemlock(); err != nil {
		slog.Error("rlimit remove memlock", "error", err)
		os.Exit(1)
	}

	denyMap, err := blocklist.LoadPinnedMap(blocklistPath)
	if err != nil {
		slog.Error("open pinned blocklist map", "path", blocklistPath, "error", err)
		os.Exit(1)
	}
	defer denyMap.Close()

	allowMap, err := allowlist.LoadPinnedMap(allowlistPath)
	if err != nil {
		slog.Error("open pinned allowlist map", "path", allowlistPath, "error", err)
		os.Exit(1)
	}
	defer allowMap.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: os.Getenv("REDIS_PASS"),
	})
	defer rdb.Close()

	ctx, cancel := lifecycle.NotifyContext(context.Background())
	defer cancel()

	denyStore := blocklist.NewStore()
	allowStore := allowlist.NewStore()
	if ebpfEdgeLicensed(ctx, rdb) {
		if err := runSync(ctx, rdb, denyMap, allowMap, denyStore, allowStore); err != nil {
			slog.Warn("initial edge bpf sync failed", "error", err)
		}
	} else {
		slog.Warn("ebpf_xdp_edge module not licensed; edge-bpf-sync idle")
	}

	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("edge-bpf-sync stopped")
			return
		case <-ticker.C:
			if !ebpfEdgeLicensed(ctx, rdb) {
				continue
			}
			if err := runSync(ctx, rdb, denyMap, allowMap, denyStore, allowStore); err != nil {
				slog.Warn("edge bpf sync failed", "error", err)
			}
		}
	}
}

func ebpfEdgeLicensed(ctx context.Context, rdb *redis.Client) bool {
	enabled, err := rdb.HGet(ctx, "entitlement:deployment", "ebpf_xdp_edge").Int()
	if err != nil {
		return true // fail-open when entitlement snapshot missing (dev)
	}
	return enabled == 1
}

func runSync(ctx context.Context, rdb *redis.Client, denyMap, allowMap *ebpf.Map, denyStore *blocklist.Store, allowStore *allowlist.Store) error {
	denyAdded, denyRemoved, err := blocklist.SyncFromRedis(ctx, rdb, denyMap, denyStore)
	if err != nil {
		return err
	}
	allowAdded, allowRemoved, err := allowlist.SyncFromRedis(ctx, rdb, allowMap, allowStore)
	if err != nil {
		return err
	}
	slog.Info("edge bpf synced",
		"deny_entries", denyStore.Len(),
		"deny_added", denyAdded,
		"deny_removed", denyRemoved,
		"allow_entries", allowStore.Len(),
		"allow_added", allowAdded,
		"allow_removed", allowRemoved,
	)
	return nil
}

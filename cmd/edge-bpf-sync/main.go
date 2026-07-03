// Command edge-bpf-sync mirrors Redis shard-0 deny/allow sets into pinned XDP LPM maps.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"espx/internal/edge"
	"espx/internal/edge/allowlist"
	"espx/internal/edge/blocklist"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/redis/go-redis/v9"
)

// main polls Redis deny/allow sets and incrementally updates pinned BPF maps.
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

	ctx, cancel := signalContext()
	defer cancel()

	denyStore := blocklist.NewStore()
	allowStore := allowlist.NewStore()
	if err := runSync(ctx, rdb, denyMap, allowMap, denyStore, allowStore); err != nil {
		slog.Warn("initial edge bpf sync failed", "error", err)
	}

	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("edge-bpf-sync stopped")
			return
		case <-ticker.C:
			if err := runSync(ctx, rdb, denyMap, allowMap, denyStore, allowStore); err != nil {
				slog.Warn("edge bpf sync failed", "error", err)
			}
		}
	}
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

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()
	return ctx, cancel
}

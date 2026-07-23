// Command edge-bpf-sync mirrors Redis shard-0 deny and allow sets into pinned XDP LPM trie maps.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"espx/internal/edge"
	"espx/internal/edge/allowlist"
	"espx/internal/edge/blocklist"
	"espx/internal/edge/bpf"
	"espx/internal/edge/fingerprint"
	"espx/internal/edge/xdpstats"
	"espx/pkg/lifecycle"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	syncInterval := edge.EnvDuration("SYNC_INTERVAL", 5*time.Second)
	statsInterval := edge.EnvDuration("STATS_INTERVAL", 2*time.Second)
	violationInterval := edge.EnvDuration("VIOLATION_POLL_INTERVAL", 250*time.Millisecond)
	fingerprintInterval := edge.EnvDuration("FINGERPRINT_POLL_INTERVAL", 500*time.Millisecond)
	autobanTTL := edge.EnvDuration("AUTOBAN_TTL", 5*time.Minute)
	metricsPort := edge.EnvOr("METRICS_PORT", "9090")
	blocklistPath := edge.EnvOr("BPF_BLOCKLIST_MAP", blocklist.DefaultMapPath)
	allowlistPath := edge.EnvOr("BPF_ALLOWLIST_MAP", allowlist.DefaultMapPath)
	statsPath := edge.EnvOr("BPF_STATS_MAP", bpf.DefaultStatsMapPath)
	violationsPath := edge.EnvOr("BPF_VIOLATIONS_MAP", bpf.DefaultViolationsMapPath)
	fingerprintsPath := edge.EnvOr("BPF_FINGERPRINTS_MAP", bpf.DefaultFingerprintsMapPath)
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

	statsMap, err := bpf.LoadPinnedStatsMap(statsPath)
	if err != nil {
		slog.Warn("open pinned stats map; xdp metrics disabled", "path", statsPath, "error", err)
	} else {
		defer statsMap.Close()
	}

	var violationReader *ringbuf.Reader
	violationsMap, err := bpf.LoadPinnedViolationsMap(violationsPath)
	if err != nil {
		slog.Warn("open pinned violations ringbuf; autoban disabled", "path", violationsPath, "error", err)
	} else {
		defer violationsMap.Close()
		violationReader, err = ringbuf.NewReader(violationsMap)
		if err != nil {
			slog.Warn("create violations ringbuf reader", "error", err)
		} else {
			defer violationReader.Close()
		}
	}

	var fingerprintReader *ringbuf.Reader
	fingerprintsMap, err := bpf.LoadPinnedFingerprintsMap(fingerprintsPath)
	if err != nil {
		slog.Warn("open pinned fingerprints ringbuf; ivt staging disabled", "path", fingerprintsPath, "error", err)
	} else {
		defer fingerprintsMap.Close()
		fingerprintReader, err = ringbuf.NewReader(fingerprintsMap)
		if err != nil {
			slog.Warn("create fingerprints ringbuf reader", "error", err)
		} else {
			defer fingerprintReader.Close()
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: os.Getenv("REDIS_PASS"),
	})
	defer rdb.Close()

	ctx, cancel := lifecycle.NotifyContext(context.Background())
	defer cancel()

	go serveMetrics(ctx, metricsPort)

	denyStore := blocklist.NewStore()
	allowStore := allowlist.NewStore()
	var lastStats []uint64

	violationHandler := bpf.NewViolationHandler(func(evt bpf.ViolationEvent) error {
		ip := bpf.HostIPv4(evt.SrcIP)
		if err := blocklist.RecordAutoBan(ctx, rdb, ip, autobanTTL); err != nil {
			return err
		}
		slog.Info("xdp autoban recorded",
			"ip", ip,
			"reason", bpf.ViolationReasonLabel(evt.Reason),
			"ttl", autobanTTL.String(),
		)
		return nil
	})

	fingerprintHandler := bpf.NewFingerprintHandler(func(evt bpf.FingerprintEvent) error {
		return fingerprint.Record(ctx, rdb, fingerprint.Entry{
			IP:      bpf.HostIPv4(evt.SrcIP),
			TCPHash: evt.TCPHash,
			TTL:     evt.TTL,
			Window:  evt.Window,
			MSS:     evt.MSS,
			SeenAt:  time.Now().UTC(),
		})
	})

	if ebpfEdgeLicensed(ctx, rdb) {
		if err := runSync(ctx, rdb, denyMap, allowMap, denyStore, allowStore); err != nil {
			slog.Warn("initial edge bpf sync failed", "error", err)
		}
	} else {
		slog.Warn("ebpf_xdp_edge module not licensed; edge-bpf-sync idle")
	}

	if statsMap != nil {
		lastStats = exportStats(ctx, rdb, statsMap, lastStats)
	}

	syncTicker := time.NewTicker(syncInterval)
	defer syncTicker.Stop()

	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()

	violationTicker := time.NewTicker(violationInterval)
	defer violationTicker.Stop()

	fingerprintTicker := time.NewTicker(fingerprintInterval)
	defer fingerprintTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("edge-bpf-sync stopped")
			return
		case <-violationTicker.C:
			if violationReader == nil || !ebpfEdgeLicensed(ctx, rdb) {
				continue
			}
			n, err := violationHandler.Drain(violationReader, violationInterval)
			if err != nil {
				slog.Warn("violation ringbuf drain failed", "error", err)
				continue
			}
			if n > 0 {
				if err := runSync(ctx, rdb, denyMap, allowMap, denyStore, allowStore); err != nil {
					slog.Warn("post-violation bpf sync failed", "error", err)
				}
			}
		case <-fingerprintTicker.C:
			if fingerprintReader == nil || !ebpfEdgeLicensed(ctx, rdb) {
				continue
			}
			if _, err := fingerprintHandler.Drain(fingerprintReader, fingerprintInterval); err != nil {
				slog.Warn("fingerprint ringbuf drain failed", "error", err)
			}
		case <-statsTicker.C:
			if statsMap != nil {
				lastStats = exportStats(ctx, rdb, statsMap, lastStats)
			}
		case <-syncTicker.C:
			if !ebpfEdgeLicensed(ctx, rdb) {
				continue
			}
			if err := runSync(ctx, rdb, denyMap, allowMap, denyStore, allowStore); err != nil {
				slog.Warn("edge bpf sync failed", "error", err)
			}
		}
	}
}

func serveMetrics(ctx context.Context, port string) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("edge-bpf-sync metrics listening", "port", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("metrics server failed", "error", err)
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

func exportStats(ctx context.Context, rdb *redis.Client, statsMap *ebpf.Map, last []uint64) []uint64 {
	last = bpf.ExportStatsToPrometheus(statsMap, last)
	totals, err := bpf.AggregateStats(statsMap)
	if err != nil {
		return last
	}
	snap := bpf.BuildSnapshot(totals)
	snap.UpdatedAt = time.Now().UTC()
	if err := xdpstats.WriteRedis(ctx, rdb, snap); err != nil {
		slog.Warn("write xdp stats snapshot", "error", err)
	}
	return last
}

// Command broker runs the ESPX log ingest broker with on-disk segments and Redis coordination.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"espx/internal/config"
	"espx/pkg/broker/log"
	"espx/pkg/broker/server"
)

// main runs the log broker as a standalone node because durable ingest segments need local disk and Redis leader election.
func main() {
	if len(os.Args) > 2 && os.Args[1] == "--health-probe" {
		resp, err := http.Get(os.Args[2])
		if err != nil || resp.StatusCode != 200 {
			os.Exit(1)
		}
		os.Exit(0)
	}

	addr := flag.String("addr", "127.0.0.1:9092", "Address for gnet TCP traffic")
	healthAddr := flag.String("health-addr", "127.0.0.1:8081", "Address for HTTP health checks")
	dataDir := flag.String("data-dir", "/tmp/espx-broker", "Data directory for segments")
	nodeID := flag.String("node-id", "broker-1", "Unique node ID")
	redisURL := flag.String("redis-url", "redis://127.0.0.1:6379/0", "Redis URL for coordination")
	maxSegSize := flag.Int64("max-seg-size", 64*1024*1024, "Maximum segment size in bytes")
	indexInterval := flag.Int64("index-interval", 4096, "Index interval in bytes")
	durabilityMode := flag.String("durability", "async", "Durability mode: async|group|sync")
	flushInterval := flag.Duration("flush-interval", 100*time.Millisecond, "Background fsync interval for async/group modes")
	groupCommitRecords := flag.Int64("group-commit-records", 64, "Records between fsyncs in group mode")
	maxConnections := flag.Int64("max-connections", 0, "Max concurrent TCP clients (0 = unlimited)")
	retentionBytes := flag.Int64("retention-bytes", 10*1024*1024*1024, "Max on-disk bytes per topic partition (0 = disabled)")
	retentionAge := flag.Duration("retention-age", 24*time.Hour, "Max age of sealed segments (0 = disabled)")
	retentionCheck := flag.Duration("retention-check-interval", 5*time.Minute, "How often to evaluate segment retention")
	retentionSafety := flag.Uint64("retention-safety-messages", 10000, "Messages kept below head before sealed segments may be deleted")
	leaderLeaseTTL := flag.Duration("leader-lease-ttl", 15*time.Second, "Redis leader key TTL")
	coordInterval := flag.Duration("coord-interval", 3*time.Second, "Leader election and renew loop interval")
	renewFailThreshold := flag.Int("renew-fail-threshold", 3, "Consecutive lease renew failures before proactive step-down")
	electionDebounce := flag.Duration("election-debounce", 2*time.Second, "Suppress epoch bump when same node reclaims leadership quickly")
	flag.Parse()

	durMode, err := log.ParseDurabilityMode(*durabilityMode)
	if err != nil {
		slog.Error("Invalid durability mode", "error", err)
		os.Exit(1)
	}
	durability := log.DurabilityConfig{
		Mode:               durMode,
		FlushInterval:      *flushInterval,
		GroupCommitRecords: *groupCommitRecords,
	}

	slog.Info("Starting ESPX Broker", "node_id", *nodeID, "addr", *addr, "health_addr", *healthAddr, "durability", *durabilityMode)

	srv := server.NewServer(*addr, *dataDir, *maxSegSize, *indexInterval)
	srv.SetDurability(durability)
	srv.SetMaxConnections(*maxConnections)
	srv.SetShutdownTimeout(config.LifecycleShutdownTimeout())
	srv.SetHealthAddr(*healthAddr)
	srv.SetRetentionPolicy(log.RetentionPolicy{
		MaxAge:         *retentionAge,
		MaxBytes:       *retentionBytes,
		SafetyMessages: *retentionSafety,
	})
	srv.SetRetentionCheckInterval(*retentionCheck)

	if err := srv.Start(); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	coord, err := server.NewCoordinatorWithConfig(*nodeID, srv.Addr(), *redisURL, srv, server.CoordConfig{
		LeaseTTL:           *leaderLeaseTTL,
		Interval:           *coordInterval,
		RenewFailThreshold: *renewFailThreshold,
		DebounceWindow:     *electionDebounce,
	})
	if err != nil {
		slog.Error("Failed to initialize coordinator", "error", err)
		srv.Stop()
		os.Exit(1)
	}

	srv.SetCoordinator(coord)
	coord.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("ESPX Broker running. Press Ctrl+C to exit.")
	<-sigChan

	slog.Info("Shutting down ESPX Broker...")
	coord.Stop()
	srv.Stop()
	slog.Info("Shutdown complete.")
}

// Command broker wires the mmap log ingest broker with on-disk segments and Redis leader election.
package main

import (
	"flag"
	"log/slog"
	"os"

	"espx/internal/config"
	"espx/pkg/lifecycle"
	"espx/pkg/broker/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9092", "Address for gnet TCP traffic")
	healthAddr := flag.String("health-addr", "127.0.0.1:8081", "Address for HTTP health checks")
	dataDir := flag.String("data-dir", "/tmp/espx-broker", "Data directory for segments")
	nodeID := flag.String("node-id", "broker-1", "Unique node ID")
	redisURL := flag.String("redis-url", "redis://127.0.0.1:6379/0", "Redis URL for coordination")
	maxSegSize := flag.Int64("max-seg-size", 64*1024*1024, "Maximum segment size in bytes")
	indexInterval := flag.Int64("index-interval", 4096, "Index interval in bytes")
	flag.Parse()

	slog.Info("Starting ESPX Broker", "node_id", *nodeID, "addr", *addr, "health_addr", *healthAddr)

	srv := server.NewServer(*addr, *dataDir, *maxSegSize, *indexInterval)
	srv.SetHealthAddr(*healthAddr)
	srv.SetShutdownTimeout(config.LifecycleShutdownTimeout())

	if err := srv.Start(); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	coord, err := server.NewCoordinator(*nodeID, srv.Addr(), *redisURL, srv)
	if err != nil {
		slog.Error("Failed to initialize coordinator", "error", err)
		srv.Stop()
		os.Exit(1)
	}

	srv.SetCoordinator(coord)
	coord.Start()

	slog.Info("ESPX Broker running")
	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal", "signal", sig.String(), "node_id", *nodeID)

	slog.Info("Shutting down ESPX Broker...")
	srv.Stop()
	coord.Stop()
	slog.Info("Shutdown complete.")
}

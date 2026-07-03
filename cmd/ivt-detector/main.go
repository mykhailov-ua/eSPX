// Command ivt-detector analyzes ClickHouse telemetry for IVT botnet clusters and enqueues blacklist:fraud via management.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ivtdetector"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadIVTDetector()
	if err != nil {
		slog.Error("failed to load ivt detector config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.Connect(ctx, string(cfg.DBDSN), cfg.DBMaxConns, cfg.DBMinConns)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	chConn, err := database.ConnectClickHouse(ctx, string(cfg.CHDSN))
	if err != nil {
		slog.Error("failed to connect to clickhouse", "error", err)
		os.Exit(1)
	}
	defer func() { _ = chConn.Close() }()

	analyzerCfg := ivtdetector.AnalyzerConfig{
		Window:          time.Duration(cfg.WindowSec) * time.Second,
		MinClicks:       cfg.MinClicks,
		MinImpressions:  cfg.MinImpressions,
		ClickToImpRatio: cfg.ClickToImpRatio,
		MinIPsPerUA:     cfg.MinIPsPerUA,
		MinEventsPerIP:  cfg.MinClicks,
	}
	detectorCfg := ivtdetector.DetectorConfig{
		ScanInterval:       time.Duration(cfg.ScanIntervalMs) * time.Millisecond,
		OutboxPendingLimit: cfg.OutboxPendingLimit,
		ManagementTimeout:  time.Duration(cfg.ManagementTimeoutMs) * time.Millisecond,
		Analyzer:           analyzerCfg,
	}

	detector := ivtdetector.NewDetector(
		ivtdetector.NewAnalyzer(chConn, analyzerCfg),
		ivtdetector.NewIdempotencyStore(pool),
		ivtdetector.NewManagementClient(cfg.ManagementURL, string(cfg.AdminAPIKey), detectorCfg.ManagementTimeout),
		pool,
		detectorCfg,
	)

	slog.Info("starting ivt detector",
		"management_url", cfg.ManagementURL,
		"scan_interval_ms", cfg.ScanIntervalMs,
		"window_sec", cfg.WindowSec,
		"outbox_pending_limit", cfg.OutboxPendingLimit,
	)

	if err := detector.RunLoop(ctx); err != nil && err != context.Canceled {
		slog.Error("ivt detector stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("ivt detector shutdown complete")
}

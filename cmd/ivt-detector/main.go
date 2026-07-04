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

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	if !cfg.IVTDetectorEnabled() {
		slog.Error("ivt detector requires IVT_DETECTOR_ENABLED=true and CH_DSN")
		os.Exit(1)
	}
	if string(cfg.AdminAPIKey) == "" {
		slog.Error("ADMIN_API_KEY is required for management blacklist enqueue")
		os.Exit(1)
	}

	managementURL := cfg.ManagementURL
	if managementURL == "" {
		managementURL = "http://127.0.0.1:" + cfg.ManagementPort
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.Connect(ctx, string(cfg.DBDSN), cfg.DBTrackerMaxConns, cfg.DBMinConns)
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
		Window:          time.Duration(cfg.IVT.WindowSec) * time.Second,
		MinClicks:       cfg.IVT.MinClicks,
		MinImpressions:  cfg.IVT.MinImpressions,
		ClickToImpRatio: cfg.IVT.ClickToImpRatio,
		MinIPsPerUA:     cfg.IVT.MinIPsPerUA,
		MinEventsPerIP:  cfg.IVT.MinClicks,
	}
	detectorCfg := ivtdetector.DetectorConfig{
		ScanInterval:       time.Duration(cfg.IVT.ScanIntervalMs) * time.Millisecond,
		OutboxPendingLimit: cfg.IVT.OutboxPendingLimit,
		Analyzer:           analyzerCfg,
	}

	detector := ivtdetector.NewDetector(
		ivtdetector.NewAnalyzer(chConn, analyzerCfg),
		ivtdetector.NewIdempotencyStore(pool),
		ivtdetector.NewManagementClient(managementURL, string(cfg.AdminAPIKey), 10*time.Second),
		pool,
		detectorCfg,
	)

	slog.Info("starting ivt detector",
		"management_url", managementURL,
		"scan_interval_ms", cfg.IVT.ScanIntervalMs,
		"window_sec", cfg.IVT.WindowSec,
	)

	if err := detector.RunLoop(ctx); err != nil && err != context.Canceled {
		slog.Error("ivt detector stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("ivt detector shutdown complete")
}

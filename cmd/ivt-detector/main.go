// Command ivt-detector wires ClickHouse botnet analysis and management blacklist enqueue.
package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/fraudscoring"
	"espx/internal/ivtdetector"
	"espx/pkg/lifecycle"
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

	ctx, stop := lifecycle.NotifyContext(context.Background())
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

	var blocker ivtdetector.BlacklistBlocker
	settlementTarget := cfg.SettlementServerHost + ":" + cfg.SettlementServerPort
	if string(cfg.SettlementInternalToken) != "" {
		grpcClient, conn, grpcErr := ivtdetector.NewGRPCManagementClient(settlementTarget, string(cfg.SettlementInternalToken))
		if grpcErr != nil {
			slog.Error("failed to connect to management settlement gRPC", "error", grpcErr)
			os.Exit(1)
		}
		defer func() { _ = conn.Close() }()
		blocker = grpcClient
		slog.Info("ivt detector using settlement gRPC BlockIP", "target", settlementTarget)
	} else {
		managementURL := cfg.ManagementURL
		if managementURL == "" {
			managementURL = "http://127.0.0.1:" + cfg.ManagementPort
		}
		if string(cfg.AdminAPIKey) == "" {
			slog.Error("SETTLEMENT_INTERNAL_TOKEN or ADMIN_API_KEY required for blacklist enqueue")
			os.Exit(1)
		}
		blocker = ivtdetector.NewManagementClient(managementURL, string(cfg.AdminAPIKey), 10*time.Second)
		slog.Warn("ivt detector using legacy HTTP blacklist; prefer SETTLEMENT_INTERNAL_TOKEN")
	}

	asn := &ivtdetector.StaticASNClassifier{
		DatacenterPrefixes: strings.Split(os.Getenv("IVT_DATACENTER_PREFIXES"), ","),
	}

	var scorer fraudscoring.Scorer
	if cfg.FraudScoringEnabled() && !cfg.FraudScorerStandalone() {
		var err error
		scorer, err = fraudscoring.NewLGBMScorer(cfg.FraudScoring.ModelPath)
		if err != nil {
			slog.Error("failed to initialize fraud scorer", "error", err, "path", cfg.FraudScoring.ModelPath)
			os.Exit(1)
		}
		slog.Info("initialized embedded fraud scorer", "path", cfg.FraudScoring.ModelPath)
	} else if cfg.FraudScorerStandalone() {
		slog.Info("FRAUD_SCORER_STANDALONE enabled; skipping embedded scorer in ivt-detector")
	}

	registry := ivtdetector.NewAnalyzerRegistry(chConn, pool, analyzerCfg, asn, scorer, cfg.FraudScoring.BatchSize)

	detector := ivtdetector.NewDetector(
		registry,
		ivtdetector.NewIdempotencyStore(pool),
		blocker,
		pool,
		detectorCfg,
	)

	slog.Info("starting ivt detector",
		"scan_interval_ms", cfg.IVT.ScanIntervalMs,
		"window_sec", cfg.IVT.WindowSec,
	)

	if err := detector.RunLoop(ctx); err != nil && err != context.Canceled {
		slog.Error("ivt detector stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("ivt detector shutdown complete")
}

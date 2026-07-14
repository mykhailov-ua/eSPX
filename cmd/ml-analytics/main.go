// Command ml-analytics runs the standalone ML scoring worker and registers newly trained models.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ivtdetector"
	"espx/internal/mlanalytics"
	"espx/pkg/lifecycle"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// In standalone mode, we require ML_ANALYTICS_ENABLED=true
	if !cfg.MLAnalyticsEnabled() {
		slog.Error("ml-analytics requires ML_ANALYTICS_ENABLED=true")
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

	// Start background model registration watcher
	go watchAndRegisterModels(ctx, pool)

	// Initialize ML Scorer
	var scorer mlanalytics.Scorer
	scorer, err = mlanalytics.NewLGBMScorer(cfg.ML.ModelPath)
	if err != nil {
		slog.Error("failed to initialize ML scorer", "error", err, "path", cfg.ML.ModelPath)
		os.Exit(1)
	}
	slog.Info("initialized ML scorer for shadow scoring", "path", cfg.ML.ModelPath)

	analyzerCfg := ivtdetector.AnalyzerConfig{
		Window:          time.Duration(cfg.IVT.WindowSec) * time.Second,
		MinClicks:       cfg.IVT.MinClicks,
		MinImpressions:  cfg.IVT.MinImpressions,
		ClickToImpRatio: cfg.IVT.ClickToImpRatio,
		MinIPsPerUA:     cfg.IVT.MinIPsPerUA,
		MinEventsPerIP:  cfg.IVT.MinClicks,
	}
	detectorCfg := ivtdetector.DetectorConfig{
		ScanInterval:       time.Duration(cfg.ML.ScanIntervalMs) * time.Millisecond,
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
		slog.Info("ml-analytics using settlement gRPC BlockIP", "target", settlementTarget)
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
		slog.Warn("ml-analytics using legacy HTTP blacklist; prefer SETTLEMENT_INTERNAL_TOKEN")
	}

	// Create a rule registry containing ONLY the ML shadow scoring rule
	registry := ivtdetector.NewRuleRegistry()
	registry.Register(ivtdetector.NewMLRule(chConn, pool, scorer, cfg.ML.BatchSize))

	detector := ivtdetector.NewDetector(
		registry,
		ivtdetector.NewIdempotencyStore(pool),
		blocker,
		pool,
		detectorCfg,
	)

	slog.Info("starting standalone ml-analytics worker",
		"scan_interval_ms", cfg.ML.ScanIntervalMs,
		"window_sec", cfg.IVT.WindowSec,
	)

	if err := detector.RunLoop(ctx); err != nil && err != context.Canceled {
		slog.Error("ml-analytics worker stopped with error", "error", err)
		os.Exit(1)
	}
}

// watchAndRegisterModels periodically scans the artifact store for newly trained models.
func watchAndRegisterModels(ctx context.Context, pool *pgxpool.Pool) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := scanAndRegister(ctx, pool); err != nil {
				slog.Error("failed to scan and register models", "error", err)
			}
		}
	}
}

func scanAndRegister(ctx context.Context, pool *pgxpool.Pool) error {
	artifactDir := "var/ml/artifacts"
	modelPath := filepath.Join(artifactDir, "model.txt")
	metadataPath := filepath.Join(artifactDir, "metadata.json")

	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		return nil // No model file yet
	}

	// 1. Calculate SHA256 of model.txt
	hash, err := calculateSHA256(modelPath)
	if err != nil {
		return fmt.Errorf("calculate sha256 of model: %w", err)
	}

	// 2. Check if hash already exists in ml_model_versions
	var exists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ml_model_versions WHERE artifact_hash = $1)", hash).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check model version existence: %w", err)
	}

	if exists {
		return nil // Already registered
	}

	// 3. Read metadata.json if available
	var version string = "v" + hash[:8]
	var metricsJSON []byte = []byte("{}")

	if _, err := os.Stat(metadataPath); err == nil {
		data, err := os.ReadFile(metadataPath)
		if err == nil {
			var meta struct {
				Version string          `json:"version"`
				Metrics json.RawMessage `json:"metrics"`
			}
			if err := json.Unmarshal(data, &meta); err == nil {
				if meta.Version != "" {
					version = meta.Version
				}
				if meta.Metrics != nil {
					metricsJSON = meta.Metrics
				}
			}
		}
	}

	// 4. Check if there are any active syncing versions to avoid concurrent syncs
	var syncingExists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ml_model_versions WHERE status = 'SYNCING')").Scan(&syncingExists)
	if err != nil {
		return fmt.Errorf("check syncing model version: %w", err)
	}
	if syncingExists {
		slog.Info("new model found but another sync is already in progress; skipping registration", "version", version)
		return nil
	}

	// 5. Register new model version with status 'SYNCING' to trigger M-ML3 orchestrator
	slog.Info("registering newly trained model version", "version", version, "hash", hash)
	_, err = pool.Exec(ctx, `
		INSERT INTO ml_model_versions (id, artifact_hash, metrics_json, status, created_at)
		VALUES ($1, $2, $3, 'SYNCING', NOW())
		ON CONFLICT (id) DO NOTHING`,
		version, hash, metricsJSON)
	if err != nil {
		return fmt.Errorf("insert model version: %w", err)
	}

	return nil
}

func calculateSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

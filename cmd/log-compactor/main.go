// Command log-compactor downsamples warm-tier audit logs from rotated tracker segments.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/pkg/lifecycle"
	"espx/internal/logcompactor"
	"espx/pkg/logger"
)

func main() {
	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(slogLogger)

	cfg, err := config.LoadLogCompactor()
	if err != nil {
		slog.Error("failed to load log compactor config", "error", err)
		os.Exit(1)
	}

	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()

	timeouts := lifecycle.TimeoutsFromEnv()
	metricsSrv := lifecycle.StartMetrics("127.0.0.1:" + cfg.MetricsPort)
	defer func() {
		if err := metricsSrv.Shutdown(timeouts.Wait); err != nil {
			slog.Error("compactor metrics server shutdown failed", "error", err)
		}
	}()

	store, err := newTierStore(ctx, cfg)
	if err != nil {
		slog.Error("failed to initialize tier store", "backend", cfg.Backend, "error", err)
		os.Exit(1)
	}

	logcompactor.RegisterMetrics()

	var decryptKey []byte
	if passphrase := os.Getenv("LOG_ENCRYPTION_KEY"); passphrase != "" {
		decryptKey = logger.DeriveKey(passphrase)
	}

	hotMinAge := time.Duration(cfg.HotMinAgeHours) * time.Hour
	localStore, _ := store.(*logcompactor.LocalTierStore)
	if localStore == nil {
		if s3Store, ok := store.(*logcompactor.S3TierStore); ok {
			localStore = s3Store.LocalScratch()
		}
	}
	checkpoint := logcompactor.NewCheckpointStore(cfg.CheckpointPath)

	var compactorOpts []logcompactor.CompactorOption
	if cfg.LeaderElection {
		compactorOpts = append(compactorOpts, logcompactor.WithLeaderLock(logcompactor.NewFileLeaderLock(cfg.LeaderLockPath)))
	}

	compactor := logcompactor.NewCompactor(logcompactor.Config{
		HotMinAge:                hotMinAge,
		SampleRate:               uint64(cfg.SampleRate),
		DeleteSourceAfterCompact: cfg.DeleteSourceAfterCompact,
		CheckpointPath:           cfg.CheckpointPath,
		WorkInterval:             time.Duration(cfg.WorkIntervalHours) * time.Hour,
		WarmDir:                  cfg.WarmDir,
		SourceDir:                cfg.SourceDir,
	}, store, checkpoint, decryptKey, compactorOpts...)

	slog.Info("starting log compactor",
		"backend", cfg.Backend,
		"source_dir", cfg.SourceDir,
		"warm_dir", cfg.WarmDir,
		"hot_min_age", hotMinAge,
		"sample_rate", cfg.SampleRate,
		"delete_source", cfg.DeleteSourceAfterCompact,
		"cold_enabled", cfg.ColdEnabled,
		"leader_election", cfg.LeaderElection,
	)

	errCh := make(chan error, 2)
	workers := 1
	go func() {
		errCh <- compactor.Run(ctx)
	}()

	if cfg.ColdEnabled {
		if localStore == nil {
			slog.Error("cold tier requires local or s3 scratch backend")
			os.Exit(1)
		}
		conn, err := database.ConnectClickHouse(ctx, cfg.CHDSN)
		if err != nil {
			slog.Error("failed to connect to clickhouse for cold tier", "error", err)
			os.Exit(1)
		}
		defer conn.Close()

		coldCheckpoint := logcompactor.NewCheckpointStore(cfg.ColdCheckpointPath)
		coldRolluper := logcompactor.NewColdRolluper(logcompactor.ColdConfig{
			WarmMinAge:            time.Duration(cfg.ColdWarmMinAgeDays) * 24 * time.Hour,
			WorkInterval:          time.Duration(cfg.ColdWorkIntervalHours) * time.Hour,
			WarmDir:               localStore.WarmDir,
			DeleteWarmAfterRollup: cfg.DeleteWarmAfterCold,
		}, localStore, coldCheckpoint, logcompactor.NewClickHouseRollupInserter(conn))

		workers++
		go func() {
			errCh <- coldRolluper.Run(ctx)
		}()
	}

	var runErr error
	for range workers {
		if err := <-errCh; err != nil && err != context.Canceled && runErr == nil {
			runErr = err
			stop()
		}
	}
	if runErr != nil {
		slog.Error("log compactor stopped with error", "error", runErr)
		os.Exit(1)
	}

	slog.Info("log compactor shutdown complete")
}

func newTierStore(ctx context.Context, cfg config.LogCompactor) (logcompactor.TierStore, error) {
	switch cfg.Backend {
	case "s3":
		return logcompactor.NewS3TierStore(ctx, logcompactor.S3Config{
			Region:         cfg.S3Region,
			Bucket:         cfg.S3Bucket,
			HotPrefix:      cfg.S3HotPrefix,
			WarmPrefix:     cfg.S3WarmPrefix,
			ScratchDir:     cfg.S3ScratchDir,
			Endpoint:       cfg.S3Endpoint,
			ForcePathStyle: cfg.S3ForcePathStyle,
		})
	default:
		return logcompactor.NewLocalTierStore(cfg.SourceDir, cfg.WarmDir), nil
	}
}

// Command log-evacuator ships rotated tracker segments to S3 with checkpointed at-least-once delivery.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"espx/internal/config"
	"espx/internal/logevacuator"
	"espx/pkg/lifecycle"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadLogEvacuator()
	if err != nil {
		slog.Error("failed to load log evacuator config", "error", err)
		os.Exit(1)
	}

	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()

	store, err := logevacuator.NewS3Store(ctx, logevacuator.S3Config{
		Region:             cfg.S3Region,
		Bucket:             cfg.S3Bucket,
		Prefix:             cfg.S3Prefix,
		Endpoint:           cfg.S3Endpoint,
		ForcePathStyle:     cfg.S3ForcePathStyle,
		MultipartThreshold: cfg.MultipartThreshold,
	})
	if err != nil {
		slog.Error("failed to initialize s3 store", "error", err)
		os.Exit(1)
	}

	evac, err := logevacuator.NewEvacuator(logevacuator.Config{
		LogDir:                 cfg.LogDir,
		CheckpointPath:         cfg.CheckpointPath,
		ScanInterval:           time.Duration(cfg.ScanIntervalMs) * time.Millisecond,
		RequireCompactorMarker: cfg.RequireCompactorMarker,
	}, store)
	if err != nil {
		slog.Error("failed to initialize evacuator", "error", err)
		os.Exit(1)
	}

	slog.Info("starting log evacuator",
		"log_dir", cfg.LogDir,
		"bucket", cfg.S3Bucket,
		"prefix", cfg.S3Prefix,
		"region", cfg.S3Region,
	)

	if err := evac.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("log evacuator stopped with error", "error", err)
		os.Exit(1)
	}

	slog.Info("log evacuator shutdown complete")
}

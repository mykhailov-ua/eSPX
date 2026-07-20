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
	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/management"
	"espx/internal/marginguard"
)

func main() {
	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(slogLogger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Connect to Postgres
	pool, err := database.Connect(ctx, string(cfg.DBDSN), 10, 2)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	queries := db.New(pool)
	registry := ingestion.NewRegistry(queries)
	registry.SetPool(pool)
	if _, err := registry.Sync(ctx); err != nil {
		slog.Warn("initial campaign registry sync failed", "error", err)
	}
	registry.StartSync(ctx, time.Duration(cfg.RegistrySyncIntervalMs)*time.Millisecond)

	notifier, err := management.NewNotifierClient(cfg)
	if err != nil {
		slog.Warn("notifier client initialization failed", "error", err)
	}
	if notifier != nil {
		defer notifier.Close()
	}

	chRead, err := database.ConnectCHReadonly(ctx, string(cfg.CHReadonlyDSN))
	if err != nil {
		slog.Error("failed to connect to clickhouse readonly", "error", err)
		os.Exit(1)
	}
	defer chRead.Close()

	chQuery := database.NewCHQuery(chRead, database.CHQueryConfig{})

	// 3. Start worker
	worker := marginguard.NewWorker(pool, chQuery, cfg, registry, notifier)

	// Evaluation interval: 60s as per spec
	go worker.Start(ctx, 60*time.Second)

	slog.Info("margin guard binary started")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down margin guard")
}

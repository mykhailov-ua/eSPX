package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/postback"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := database.Connect(ctx, string(cfg.DBDSN), cfg.DBTrackerMaxConns, cfg.DBMinConns)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	key := []byte(os.Getenv("POSTBACK_ENCRYPTION_KEY"))
	worker := postback.NewPostbackWorker(pool, key)

	slog.Info("starting postback-sender daemon")
	go worker.Start(ctx, 5*time.Second)

	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal, shutting down postback-sender daemon", "signal", sig.String())
	cancel()
	slog.Info("postback-sender daemon shutdown complete")
}

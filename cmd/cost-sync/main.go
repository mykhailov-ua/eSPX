package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"espx/internal/config"
	"espx/internal/costsync"
	"espx/internal/database"
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

	key := []byte(os.Getenv("COST_SYNC_ENCRYPTION_KEY"))
	if len(key) == 0 {
		key = []byte(os.Getenv("POSTBACK_ENCRYPTION_KEY"))
	}

	workerOpts := []costsync.WorkerOption{}
	if cfg.ClickHouseEnabled() {
		chConn, err := database.ConnectClickHouse(ctx, string(cfg.CHDSN))
		if err != nil {
			slog.Error("failed to connect to clickhouse", "error", err)
			os.Exit(1)
		}
		defer chConn.Close()
		workerOpts = append(workerOpts, costsync.WithClickHouse(costsync.NewClickHouseInserter(chConn)))
		slog.Info("cost-sync clickhouse snapshots enabled")
	}

	if os.Getenv("META_APP_ID") != "" && os.Getenv("META_APP_SECRET") != "" {
		workerOpts = append(workerOpts, costsync.WithOAuthRefresher("facebook", &costsync.MetaOAuthRefresher{
			AppID:     os.Getenv("META_APP_ID"),
			AppSecret: os.Getenv("META_APP_SECRET"),
		}))
	}
	if os.Getenv("GOOGLE_OAUTH_CLIENT_ID") != "" && os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET") != "" {
		workerOpts = append(workerOpts, costsync.WithOAuthRefresher("google", &costsync.GoogleOAuthRefresher{
			ClientID:     os.Getenv("GOOGLE_OAUTH_CLIENT_ID"),
			ClientSecret: os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET"),
		}))
	}

	worker := costsync.NewWorker(pool, key, workerOpts...)

	slog.Info("starting cost-sync daemon")
	go worker.Start(ctx)

	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal, shutting down cost-sync daemon", "signal", sig.String())
	cancel()
	time.Sleep(500 * time.Millisecond)
	slog.Info("cost-sync daemon shutdown complete")
}

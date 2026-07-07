// Command auth wires the session and credential gRPC service in a dedicated process.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"espx/internal/auth"
	"espx/internal/auth/db"
	"espx/internal/auth/pb"
	"espx/internal/config"
	"espx/internal/database"
	"espx/pkg/lifecycle"

	google_grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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

	rdb, err := database.ConnectRedisShard(ctx, cfg, 0, database.RedisShardOptions{
		PoolSize: cfg.RedisPoolSize,
	})
	if err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	repo := db.NewStore(pool)
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	if err != nil {
		slog.Error("failed to create token maker", "error", err)
		os.Exit(1)
	}

	lockoutLimiter := auth.NewLockoutLimiter(rdb)

	hasher, err := auth.NewPasswordHasher(
		uint32(cfg.Argon2Memory),
		uint32(cfg.Argon2Iterations),
		uint8(cfg.Argon2Parallelism),
	)
	if err != nil {
		slog.Error("failed to pre-compute dummy hash during password hasher initialization", "error", err)
		os.Exit(1)
	}
	authService := auth.NewService(repo, tokenMaker, hasher, lockoutLimiter, rdb)
	cleanupWorker := auth.NewSessionCleanupWorker(authService)
	var cleanupWG sync.WaitGroup
	cleanupWG.Add(1)
	go func() {
		defer cleanupWG.Done()
		cleanupWorker.Start(ctx, time.Minute)
	}()
	grpcHandler := auth.NewHandler(authService, cfg)
	timeouts := lifecycle.TimeoutsFromConfig(cfg)

	lis, err := net.Listen("tcp", ":"+cfg.AuthServerPort)
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	server := google_grpc.NewServer()
	pb.RegisterAuthServiceServer(server, grpcHandler)

	if cfg.Env != "production" {
		reflection.Register(server)
	}

	metricsSrv := lifecycle.StartMetrics(":" + cfg.AuthMetricsPort)

	slog.Info("starting auth gRPC server", "port", cfg.AuthServerPort)

	go func() {
		if err := server.Serve(lis); err != nil {
			slog.Error("gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal", "signal", sig.String())

	cancel()
	if err := lifecycle.Wait(timeouts.Wait, cleanupWG.Wait); err != nil {
		slog.Warn("auth session cleanup worker drain timed out", "error", err)
	}
	lifecycle.ShutdownGRPC(server, timeouts.Shutdown)
	if err := metricsSrv.Shutdown(timeouts.Wait); err != nil {
		slog.Error("auth metrics server shutdown failed", "error", err)
	}
}

// Command auth runs the session and credential gRPC service as its own deployable unit.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"espx/internal/auth"
	"espx/internal/auth/db"
	"espx/internal/auth/pb"
	"espx/internal/config"
	"espx/internal/database"
	google_grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// main runs auth as an isolated gRPC service because session state and password hashing must not share the tracker process.
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
	go cleanupWorker.Start(ctx, time.Minute)
	grpcHandler := auth.NewHandler(authService, cfg)

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

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{
		Addr:              ":" + cfg.AuthMetricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	go func() {
		slog.Info("starting auth metrics server", "port", cfg.AuthMetricsPort)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
		}
	}()

	slog.Info("starting auth gRPC server", "port", cfg.AuthServerPort)

	go func() {
		if err := server.Serve(lis); err != nil {
			slog.Error("gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down auth gRPC server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	cancel()

	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics server shutdown failed", "error", err)
	}

	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		slog.Info("gRPC server stopped cleanly")
	case <-shutdownCtx.Done():
		slog.Warn("gRPC graceful shutdown timed out, force stopping")
		server.Stop()
	}
}

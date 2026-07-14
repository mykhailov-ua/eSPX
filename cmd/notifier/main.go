// Command notifier wires the gRPC NotifierService and background delivery worker.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/notifier"
	"espx/internal/notifier/pb"
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

	if err := notifier.ApplyMigrations(ctx, pool); err != nil {
		slog.Error("failed to apply notifier schema migrations", "error", err)
		os.Exit(1)
	}

	notifier.RegisterMetrics()
	notifier.SetAdminBaseURL(cfg.Notifier.AdminBaseURL)
	bundle := notifier.NewProviderBundleFromConfig(cfg)
	svc := notifier.NewServiceWithOptions(pool, bundle.Providers, notifier.ServiceOptionsFromConfig(cfg))
	grpcHandler := notifier.NewHandler(svc)

	go notifier.StartQueueMetricsScraper(ctx, pool, 15*time.Second)
	go notifier.StartCircuitBreakerMetricsScraper(ctx, bundle.Breakers, 15*time.Second)

	retentionInterval := time.Duration(cfg.Notifier.RetentionIntervalHours) * time.Hour
	go notifier.NewRetentionJanitor(
		pool,
		retentionInterval,
		cfg.Notifier.RetentionSentDays,
		cfg.Notifier.RetentionFailedDays,
	).Start(ctx)

	workerInterval := time.Duration(cfg.Notifier.WorkerIntervalMs) * time.Millisecond
	worker := notifier.NewWorker(svc, workerInterval, int32(cfg.Notifier.WorkerBatchSize))
	worker.StartPool(ctx, cfg.Notifier.WorkerConcurrency)

	metricsPort := cfg.Notifier.MetricsPort
	if metricsPort == "" {
		metricsPort = "8086"
	}
	metricsSrv := lifecycle.StartMetrics(":" + metricsPort)
	timeouts := lifecycle.TimeoutsFromConfig(cfg)

	lis, err := net.Listen("tcp", ":"+cfg.Notifier.Port)
	if err != nil {
		slog.Error("failed to listen on notifier port", "port", cfg.Notifier.Port, "error", err)
		os.Exit(1)
	}

	grpcServer := google_grpc.NewServer()
	pb.RegisterNotifierServiceServer(grpcServer, grpcHandler)

	if cfg.Env != "production" {
		reflection.Register(grpcServer)
	}

	go func() {
		slog.Info("starting notifier gRPC server", "port", cfg.Notifier.Port)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("notifier gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal", "signal", sig.String())

	cancel()
	if err := lifecycle.Wait(timeouts.Wait, worker.Wait); err != nil {
		slog.Warn("notifier worker drain timed out", "error", err)
	}
	lifecycle.ShutdownGRPC(grpcServer, timeouts.Shutdown)
	if err := metricsSrv.Shutdown(timeouts.Wait); err != nil {
		slog.Error("notifier metrics server shutdown failed", "error", err)
	}

	slog.Info("notifier service shutdown complete")
}

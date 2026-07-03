// Command notifier runs the gRPC NotifierService and background queue worker.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/notifier"
	"espx/internal/notifier/pb"

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

	providers := notifier.NewProvidersFromConfig(cfg)
	svc := notifier.NewService(pool, providers)
	grpcHandler := notifier.NewHandler(svc)

	workerInterval := time.Duration(cfg.Notifier.WorkerIntervalMs) * time.Millisecond
	worker := notifier.NewWorker(svc, workerInterval, int32(cfg.Notifier.WorkerBatchSize))
	worker.Start(ctx)

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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down notifier service")

	grpcServer.GracefulStop()
	cancel()
	worker.Wait()

	slog.Info("notifier service shutdown complete")
}

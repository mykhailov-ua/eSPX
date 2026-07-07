// Command billing wires the gRPC BillingService for invoice generation and ledger reconciliation.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"

	"espx/internal/billing"
	"espx/internal/billing/pb"
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

	svc := billing.NewService(pool)
	grpcHandler := billing.NewHandler(svc, cfg)

	lis, err := net.Listen("tcp", ":"+cfg.Billing.Port)
	if err != nil {
		slog.Error("failed to listen on billing port", "port", cfg.Billing.Port, "error", err)
		os.Exit(1)
	}

	grpcServer := google_grpc.NewServer()
	pb.RegisterBillingServiceServer(grpcServer, grpcHandler)

	if cfg.Env != "production" {
		reflection.Register(grpcServer)
	}

	timeouts := lifecycle.TimeoutsFromConfig(cfg)

	go func() {
		slog.Info("starting billing gRPC server", "port", cfg.Billing.Port)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("billing gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	metricsSrv := lifecycle.StartMetrics(":" + cfg.Billing.MetricsPort)
	slog.Info("billing metrics server enabled", "port", cfg.Billing.MetricsPort)

	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal", "signal", sig.String())

	cancel()
	lifecycle.ShutdownGRPC(grpcServer, timeouts.Shutdown)
	if err := metricsSrv.Shutdown(timeouts.Shutdown); err != nil {
		slog.Error("billing metrics server shutdown failed", "error", err)
	}
	slog.Info("billing service shutdown complete")
}

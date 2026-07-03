// Command payment runs gRPC intents, Stripe webhook ingress, and settlement outbox in isolation from management.
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

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/payment"
	"espx/internal/payment/pb"

	google_grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// main runs payment as its own process so Stripe webhooks and settlement retries do not share
// the management HTTP listener or its connection pool.
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

	prov := payment.NewProvider(cfg)
	payment.LogProviderMode(cfg)

	svc := payment.NewService(pool, prov, cfg)
	grpcHandler := payment.NewHandler(svc, cfg)

	outboxWorker := payment.NewOutboxWorker(pool, cfg)
	go outboxWorker.Start(ctx, 100*time.Millisecond)

	httpServerMux := http.NewServeMux()
	payment.NewWebhookHandler(svc, cfg).RegisterRoutes(httpServerMux)
	payment.NewHTMXHandler(svc).RegisterRoutes(httpServerMux)

	httpServer := &http.Server{
		Addr:              ":" + cfg.PaymentWebhookPort,
		Handler:           httpServerMux,
		ReadHeaderTimeout: time.Duration(cfg.HttpReadHeaderTimeoutMs) * time.Millisecond,
		ReadTimeout:       time.Duration(cfg.HttpReadTimeoutMs) * time.Millisecond,
		WriteTimeout:      time.Duration(cfg.HttpWriteTimeoutMs) * time.Millisecond,
		IdleTimeout:       time.Duration(cfg.HttpIdleTimeoutMs) * time.Millisecond,
	}

	go func() {
		slog.Info("starting payment HTTP sidecar server", "port", cfg.PaymentWebhookPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP sidecar server failed", "error", err)
			os.Exit(1)
		}
	}()

	lis, err := net.Listen("tcp", ":"+cfg.PaymentServerPort)
	if err != nil {
		slog.Error("failed to listen on payment port", "error", err)
		os.Exit(1)
	}

	grpcServer := google_grpc.NewServer()
	pb.RegisterPaymentServiceServer(grpcServer, grpcHandler)

	if cfg.Env != "production" {
		reflection.Register(grpcServer)
	}

	go func() {
		slog.Info("starting payment gRPC server", "port", cfg.PaymentServerPort)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("payment gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down payment service")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP sidecar shutdown failed", "error", err)
	}

	cancel()
	outboxWorker.Wait()

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		slog.Info("gRPC server stopped cleanly")
	case <-shutdownCtx.Done():
		slog.Warn("gRPC graceful shutdown timed out, force stopping")
		grpcServer.Stop()
	}
	slog.Info("payment service shutdown complete")
}

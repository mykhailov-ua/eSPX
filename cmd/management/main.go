// Command management runs the admin API gateway, background workers, and settlement gRPC sidecar.
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

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/auth"
	"espx/internal/auth/pb"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/management"
	pb_settle "espx/internal/management/pb"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// main hosts the management HTTP API separately because admin RBAC and background workers must not share tracker resources.
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

	var rdbs []redis.UniversalClient
	rdbs, err = database.ConnectRedisShards(ctx, cfg, database.RedisShardOptions{
		PoolSize: cfg.RedisPoolSize,
	})
	if err != nil {
		slog.Error("failed to connect to redis shards", "error", err)
		os.Exit(1)
	}

	sharder := ads.NewStaticSlotSharder(len(rdbs))

	authTarget := "127.0.0.1:" + cfg.AuthServerPort
	if host := os.Getenv("AUTH_SERVER_HOST"); host != "" {
		authTarget = host + ":" + cfg.AuthServerPort
	}

	authConn, err := grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("failed to connect to auth gRPC server", "target", authTarget, "error", err)
		os.Exit(1)
	}
	defer authConn.Close()

	authClient := pb.NewAuthServiceClient(authConn)
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	if err != nil {
		slog.Error("failed to create token maker", "error", err)
		os.Exit(1)
	}

	authMiddleware := management.NewAuthMiddleware(tokenMaker, rdbs[0], cfg)
	authHandler := management.NewAuthHandler(authClient, tokenMaker, rdbs[0], cfg, authMiddleware)

	svc := management.NewService(pool, rdbs, sharder, cfg)

	queries := db.New(pool)
	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)
	var syncWorkers []*ads.SyncWorker
	for _, rdb := range rdbs {
		sw := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, time.Duration(cfg.BudgetSyncIntervalMs)*time.Millisecond)
		syncWorkers = append(syncWorkers, sw)
		svc.StartBackgroundWorker(func() {
			sw.Start(ctx)
		})
	}

	reconInterval := time.Duration(cfg.Management.ReconIntervalMs) * time.Millisecond
	svc.StartReconWorker(reconInterval)
	slog.Info("started recon worker", "interval", reconInterval)

	pacingInterval := time.Duration(cfg.Management.PacingIntervalMs) * time.Millisecond
	svc.StartPacingController(syncWorkers, pacingInterval)
	slog.Info("started pacing controller", "interval", pacingInterval)

	svc.StartAuditCleaner(management.Days(cfg.Management.RetentionDays))
	slog.Info("started audit cleaner", "retention_days", cfg.Management.RetentionDays)

	if exportPath := os.Getenv("NGINX_DENY_EXPORT_PATH"); exportPath != "" {
		nginxWorker := management.NewNginxConfigWorker(svc, exportPath)
		svc.StartBackgroundWorker(func() {
			nginxWorker.Start(ctx, time.Minute)
		})
		slog.Info("started nginx deny export worker", "path", exportPath)
	}

	paymentClient, err := management.NewPaymentClient(cfg)
	if err != nil {
		slog.Error("failed to connect to payment gRPC server", "error", err)
		os.Exit(1)
	}
	if paymentClient != nil {
		defer paymentClient.Close()
		slog.Info("payment gRPC client enabled", "target", cfg.PaymentServerHost+":"+cfg.PaymentServerPort)
	}

	billingClient, err := management.NewBillingClient(cfg)
	if err != nil {
		slog.Error("failed to connect to billing gRPC server", "error", err)
		os.Exit(1)
	}
	if billingClient != nil {
		defer billingClient.Close()
		slog.Info("billing gRPC client enabled", "target", cfg.Billing.ServerHost+":"+cfg.Billing.Port)
	}

	mgmtHandler := management.NewHandler(svc, cfg, authMiddleware, paymentClient, billingClient)

	mux := http.NewServeMux()
	management.RegisterOpsRoutes(mux, pool, rdbs)
	authHandler.RegisterRoutes(mux)
	mgmtHandler.RegisterRoutes(mux)

	corsMdl := management.NewCORSMiddleware(cfg.AllowedOrigins)
	csrfMdl := management.NewCSRFMiddleware(string(cfg.AdminAPIKey))
	gatewayHandler := corsMdl(csrfMdl(mux))

	slog.Info("starting management gateway server", "port", cfg.ManagementPort, "auth_target", authTarget)

	server := &http.Server{
		Addr:              ":" + cfg.ManagementPort,
		Handler:           gatewayHandler,
		ReadHeaderTimeout: time.Duration(cfg.HttpReadHeaderTimeoutMs) * time.Millisecond,
		ReadTimeout:       time.Duration(cfg.HttpReadTimeoutMs) * time.Millisecond,
		WriteTimeout:      time.Duration(cfg.HttpWriteTimeoutMs) * time.Millisecond,
		IdleTimeout:       time.Duration(cfg.HttpIdleTimeoutMs) * time.Millisecond,
	}

	settleLis, err := net.Listen("tcp", ":"+cfg.SettlementServerPort)
	if err != nil {
		slog.Error("failed to listen on settlement port", "error", err)
		os.Exit(1)
	}
	settleServer := grpc.NewServer()
	settleHandler := management.NewSettlementHandler(svc, cfg)
	pb_settle.RegisterSettlementServiceServer(settleServer, settleHandler)

	go func() {
		slog.Info("starting settlement gRPC server", "port", cfg.SettlementServerPort)
		if err := settleServer.Serve(settleLis); err != nil {
			slog.Error("settlement gRPC server failed", "error", err)
		}
	}()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("management server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	sig := <-stop
	slog.Info("received shutdown signal", "signal", sig.String())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("management server shutdown failed", "error", err)
	}

	settleStopped := make(chan struct{})
	go func() {
		settleServer.GracefulStop()
		close(settleStopped)
	}()

	select {
	case <-settleStopped:
		slog.Info("settlement gRPC server stopped cleanly")
	case <-time.After(5 * time.Second):
		slog.Warn("settlement gRPC graceful shutdown timed out, force stopping")
		settleServer.Stop()
	}

	svc.Close()

	for i, rdb := range rdbs {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis shard", "shard", i, "error", err)
		}
	}
	slog.Info("management server shutdown complete")
}

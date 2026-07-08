// Command management wires the admin HTTP API and control-plane background workers outside the tracker process.
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
	auth_pb "espx/internal/auth/pb"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/management"
	mgmt_pb "espx/internal/management/pb"
	"espx/internal/processor"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	authClient := auth_pb.NewAuthServiceClient(authConn)
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	if err != nil {
		slog.Error("failed to create token maker", "error", err)
		os.Exit(1)
	}

	mgmtAuthClient := management.NewAuthClient(authClient)
	authMiddleware := management.NewAuthMiddleware(tokenMaker, rdbs[0], cfg, mgmtAuthClient)
	authHandler := management.NewAuthHandler(authClient, tokenMaker, rdbs[0], cfg, authMiddleware)

	svc := management.NewService(pool, rdbs, sharder, cfg)
	svc.SetPaymentPool(pool)

	if cfg.ClickHouseEnabled() {
		chConn, err := database.ConnectClickHouse(ctx, string(cfg.CHDSN))
		if err != nil {
			slog.Error("failed to connect to clickhouse for reporting", "error", err)
			os.Exit(1)
		}
		defer chConn.Close()
		if err := processor.ApplyClickHouseMigrations(ctx, chConn); err != nil {
			slog.Error("failed to apply clickhouse migrations", "error", err)
			os.Exit(1)
		}
		svc.SetClickHouse(chConn)
		slog.Info("clickhouse reporting enabled")
	}

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

	if cfg.QuotaMode == "shadow" || cfg.QuotaMode == "live" {
		svc.StartBackgroundWorker(func() {
			management.NewQuotaManager(svc).Start(ctx)
		})
		slog.Info("started quota manager", "mode", cfg.QuotaMode, "chunk_size", cfg.QuotaChunkSize, "refill_threshold_pct", cfg.QuotaRefillThresholdPct)
	}

	if cfg.DeliveryOptimizerIntervalMs > 0 {
		optimizerInterval := time.Duration(cfg.DeliveryOptimizerIntervalMs) * time.Millisecond
		svc.StartDeliveryOptimizerWorker(syncWorkers, optimizerInterval)
		slog.Info("started delivery optimizer worker", "interval", optimizerInterval, "mab_interval_ms", cfg.MABIntervalMs)
	} else {
		pacingInterval := time.Duration(cfg.Management.PacingIntervalMs) * time.Millisecond
		svc.StartPacingController(syncWorkers, pacingInterval)
		slog.Info("started pacing controller", "interval", pacingInterval)

		if cfg.AutoscaleIntervalMs > 0 {
			autoscaleInterval := time.Duration(cfg.AutoscaleIntervalMs) * time.Millisecond
			svc.StartAutoscaleBudgetWorker(syncWorkers, autoscaleInterval)
			slog.Info("started autoscale budget worker", "interval", autoscaleInterval)
		}
	}

	svc.StartAuditCleaner(management.Days(cfg.Management.RetentionDays))
	slog.Info("started audit cleaner", "retention_days", cfg.Management.RetentionDays)

	svc.StartBackgroundWorker(func() {
		management.NewConsentRetentionWorker(svc).Start(ctx)
	})
	slog.Info("started consent retention worker", "retention_months", cfg.ConsentRetentionMonths)

	if cfg.ErasureWorkerIntervalMs > 0 {
		erasureInterval := time.Duration(cfg.ErasureWorkerIntervalMs) * time.Millisecond
		svc.StartBackgroundWorker(func() {
			management.NewErasureWorker(svc).Start(ctx, erasureInterval)
		})
		slog.Info("started privacy erasure worker", "interval", erasureInterval)
	}

	if cfg.Management.BlacklistJanitorEnabled {
		janitorInterval := time.Duration(cfg.Management.BlacklistJanitorIntervalSec) * time.Second
		svc.StartBlacklistJanitor(janitorInterval)
		slog.Info("started blacklist TTL janitor", "interval", janitorInterval)
	}

	if exportPath := os.Getenv("NGINX_DENY_EXPORT_PATH"); exportPath != "" {
		nginxWorker := management.NewNginxConfigWorker(svc, exportPath)
		svc.StartBackgroundWorker(func() {
			nginxWorker.Start(ctx, time.Minute)
		})
		slog.Info("started nginx deny export worker", "path", exportPath)
	}

	if cfg.Management.AuditExportPath != "" {
		auditWorker := management.NewAuditExportWorker(svc, cfg.Management.AuditExportPath, cfg.Management.AuditExportRetentionDays)
		svc.StartBackgroundWorker(func() {
			auditWorker.Start(ctx, 24*time.Hour)
		})
		slog.Info("started audit export worker", "path", cfg.Management.AuditExportPath, "retention_days", cfg.Management.AuditExportRetentionDays)
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

	notifierClient, err := management.NewNotifierClient(cfg)
	if err != nil {
		slog.Error("failed to connect to notifier gRPC server", "error", err)
		os.Exit(1)
	}
	if notifierClient != nil {
		defer notifierClient.Close()
		slog.Info("notifier gRPC client enabled", "target", cfg.Notifier.ServerHost+":"+cfg.Notifier.Port)
	}
	opsAlerter := management.NewOpsAlerter(notifierClient, cfg)
	if opsAlerter != nil {
		svc.SetOpsAlerter(opsAlerter)
		slog.Info("ops alerts enabled")
	}

	alertmanagerWebhook := management.NewAlertmanagerWebhook(notifierClient, cfg)

	if cfg.SlotMigrationEnabled {
		migrationInterval := time.Duration(cfg.SlotMigrationIntervalMs) * time.Millisecond
		orchestrator := management.NewSlotMigrationOrchestrator(svc, migrationInterval)
		svc.StartBackgroundWorker(func() {
			orchestrator.Start(ctx)
		})
		slog.Info("started slot migration orchestrator", "interval", migrationInterval)
	}

	mgmtHandler := management.NewHandler(svc, cfg, authMiddleware, mgmtAuthClient, paymentClient, billingClient)

	mux := http.NewServeMux()
	management.RegisterOpsRoutes(mux, pool, rdbs)
	if alertmanagerWebhook != nil {
		alertmanagerWebhook.Register(mux)
		slog.Info("alertmanager webhook adapter enabled")
	}
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

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("management server failed", "error", err)
			os.Exit(1)
		}
	}()

	settleLis, err := net.Listen("tcp", ":"+cfg.SettlementServerPort)
	if err != nil {
		slog.Error("failed to listen on settlement port", "port", cfg.SettlementServerPort, "error", err)
		os.Exit(1)
	}
	settleHandler := management.NewSettlementHandler(svc, cfg)
	settleGRPC := grpc.NewServer(grpc.UnaryInterceptor(management.SettlementGRPCMetricsInterceptor()))
	mgmt_pb.RegisterSettlementServiceServer(settleGRPC, settleHandler)
	if cfg.Env != "production" {
		reflection.Register(settleGRPC)
	}
	go func() {
		slog.Info("starting settlement gRPC server", "port", cfg.SettlementServerPort)
		if err := settleGRPC.Serve(settleLis); err != nil {
			slog.Error("settlement gRPC server failed", "error", err)
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
		settleGRPC.GracefulStop()
		close(settleStopped)
	}()
	select {
	case <-settleStopped:
		slog.Info("settlement gRPC server stopped cleanly")
	case <-shutdownCtx.Done():
		slog.Warn("settlement gRPC graceful shutdown timed out, force stopping")
		settleGRPC.Stop()
	}

	svc.Close()

	for i, rdb := range rdbs {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis shard", "shard", i, "error", err)
		}
	}
	slog.Info("management server shutdown complete")
}

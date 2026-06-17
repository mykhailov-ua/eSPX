package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/auth"
	"espx/internal/auth/pb"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/management"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// main starts the management HTTP gateway with auth, RBAC, background workers, and ops endpoints.
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
	for i, addr := range cfg.RedisAddrs {
		rdb := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    []string{addr},
			Password: string(cfg.RedisPassword),
			PoolSize: cfg.RedisPoolSize,
		})

		var rdbErr error
		for j := 0; j < 30; j++ {
			if rdbErr = rdb.Ping(ctx).Err(); rdbErr == nil {
				break
			}
			slog.Warn("waiting for redis...", "addr", addr, "error", rdbErr)
			time.Sleep(time.Second)
		}

		if rdbErr != nil {
			slog.Error("failed to connect to redis shard", "addr", addr, "error", rdbErr)
			os.Exit(1)
		}
		breaker := database.NewRedisBreaker(
			int64(cfg.RedisBreakerFailThreshold),
			int64(cfg.RedisBreakerHalfOpen),
			time.Duration(cfg.RedisBreakerOpenTimeoutMs)*time.Millisecond,
		)
		rdb.AddHook(database.NewRedisCircuitBreakerHook(breaker, strconv.Itoa(i)))
		rdbs = append(rdbs, rdb)
	}

	sharder := ads.NewJumpHashSharder(len(rdbs))

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

	mgmtHandler := management.NewHandler(svc, cfg, authMiddleware)

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

	svc.Close()

	for i, rdb := range rdbs {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis shard", "shard", i, "error", err)
		}
	}
	slog.Info("management server shutdown complete")
}

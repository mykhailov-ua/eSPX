// Command server wires a reduced gnet tracker (JumpHash sharding, no dedicated metrics listener).
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"
	"espx/pkg/lifecycle"

	"github.com/panjf2000/gnet/v2"
	"github.com/redis/go-redis/v9"
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

	queries := db.New(pool)
	registry := ingestion.NewRegistry(queries)
	registry.SetPool(pool)
	count, err := registry.Sync(ctx)
	if err != nil {
		slog.Warn("initial campaign registry sync failed", "error", err)
	} else {
		slog.Info("campaign registry loaded", "campaigns", count)
	}
	registry.StartSync(ctx, time.Duration(cfg.RegistrySyncIntervalMs)*time.Millisecond)

	var rdbs []redis.UniversalClient
	for _, addr := range cfg.RedisAddrs {
		rdb := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    []string{addr},
			Password: string(cfg.RedisPassword),
			PoolSize: cfg.RedisPoolSize,
		})

		var rdbErr error
		for i := 0; i < 30; i++ {
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
		breaker := database.NewRedisBreaker(50, 3, 5*time.Second)
		rdb.AddHook(database.NewRedisCircuitBreakerHook(breaker))
		rdbs = append(rdbs, rdb)
	}

	channel := cfg.CampaignUpdateChannel
	if channel == "" {
		channel = "campaigns:update"
	}
	registry.StartWatch(ctx, rdbs[0], channel)

	consentChannel := cfg.ConsentUpdateChannel
	if consentChannel == "" {
		consentChannel = ingestion.ConsentDefaultUpdateChannel
	}
	consentStore := ingestion.NewConsentStore(rdbs[0])
	consentStore.StartWatch(ctx, rdbs[0], consentChannel)

	campaignRepo := ingestion.NewCampaignRepo(queries)
	sharder := ingestion.NewJumpHashSharder(len(rdbs))

	var geoProvider ingestion.GeoProvider
	geoProvider, err = ingestion.NewMaxMindProvider("deploy/geoip/GeoLite2-Country.mmdb")
	if err != nil {
		slog.Warn("failed to load MaxMind DB, using mock", "error", err)
		geoProvider = &ingestion.MockGeoProvider{}
	}
	defer geoProvider.Close()

	geoFilter := ingestion.NewGeoFilter(geoProvider, registry)
	scheduleFilter := ingestion.NewScheduleFilter(registry)
	fraudFilter := ingestion.NewFraudFilter(geoProvider)
	l3Filter := ingestion.NewFraudBlacklistFilter(rdbs[0])

	settingsWatcher := ingestion.NewSettingsWatcher(rdbs, cfg)
	deviceFilter := ingestion.NewDeviceFilter(settingsWatcher)
	go settingsWatcher.Start(ctx, time.Second)

	breakerFilter := ingestion.NewEmergencyBreakerFilter(settingsWatcher)
	consentFilter := ingestion.NewConsentFilter(registry, consentStore)
	placementFilter := ingestion.NewPlacementBlacklistFilter(rdbs)

	unifiedFilter := ingestion.NewUnifiedFilter(
		rdbs,
		sharder,
		registry,
		campaignRepo,
		cfg.RateLimitPerMin,
		time.Duration(cfg.RateLimitWindowMs)*time.Millisecond,
		time.Duration(cfg.DuplicateTTLSec)*time.Second,
		time.Duration(cfg.IdempotencyTTLHrs)*time.Hour,
		cfg.ClickAmount,
		cfg.ImpressionAmount,
		cfg.RedisStreamName,
		cfg.StreamMaxLen,
	)

	entitlementsFilter := ingestion.NewEntitlementsFilter(registry, sharder, rdbs)
	filterEngine := ingestion.NewFilterEngine(
		time.Duration(cfg.FilterTimeoutMs)*time.Millisecond,
		entitlementsFilter,
		breakerFilter,
		geoFilter,
		scheduleFilter,
		placementFilter,
		l3Filter,
		fraudFilter,
		deviceFilter,
		consentFilter,
		unifiedFilter,
	)

	creativeStore := ingestion.NewBrandCreativeStore(rdbs[0])
	gnetHandler := ingestion.NewAdsPacketHandler(cfg, registry, filterEngine, pool, rdbs, sharder, cfg.FraudStreamName, creativeStore)

	slog.Info("starting ad-event-tracker via gnet", "port", cfg.ServerPort)

	go func() {
		err := gnet.Run(gnetHandler, "tcp://:"+cfg.ServerPort,
			gnet.WithMulticore(true),
			gnet.WithReusePort(true),
			gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		)
		if err != nil {
			slog.Error("gnet server failed", "error", err)
			os.Exit(1)
		}
	}()

	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal", "signal", sig.String())

	timeouts := lifecycle.TimeoutsFromConfig(cfg)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeouts.Shutdown)
	defer shutdownCancel()

	cancel()

	if err := gnetHandler.Stop(shutdownCtx); err != nil {
		slog.Error("gnet server shutdown failed", "error", err)
	}

	if err := registry.Wait(shutdownCtx); err != nil {
		slog.Error("registry wait failed", "error", err)
	}

	for i, rdb := range rdbs {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis shard", "shard", i, "error", err)
		}
	}
	slog.Info("ad-event-tracker shutdown complete")
}

// Command tracker wires gnet ingestion, Redis Lua filters, and registry sync in a dedicated process.
// Metrics and health use a separate listener so Prometheus scrapes do not run on gnet event loops.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/health"
	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/metrics"
	"espx/internal/rtb"
	"espx/pkg/logger"

	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	if len(os.Args) > 2 && os.Args[1] == "--health-probe" {
		resp, err := http.Get(os.Args[2])
		if err != nil || resp.StatusCode != 200 {
			os.Exit(1)
		}
		os.Exit(0)
	}

	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	slog.SetDefault(slogLogger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	loggerCfg := logger.Config{
		LogDir:                cfg.Logger.Dir,
		FlushBufferSize:       cfg.Logger.FlushSizeKB * 1024,
		RotateSize:            int64(cfg.Logger.RotateSizeMB) * 1024 * 1024,
		RotateInterval:        cfg.Logger.RotateInterval,
		DiskLatencyLimit:      cfg.Logger.LatencyLimit,
		PersistQueueDepth:     cfg.Logger.PersistQueueDepth,
		PersistEnqueueTimeout: cfg.Logger.PersistEnqueueTimeout,
	}
	appLogger := logger.NewLogger(loggerCfg, cfg.Logger.Shards)
	defer appLogger.Close()

	logger.RegisterMetrics()
	appLogger.StartMetricsReporter(15 * time.Second)

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
	var breakers []*database.RedisBreaker
	rdbs, breakers, err = database.ConnectRedisShards(ctx, cfg, database.RedisShardOptions{
		PoolSize:         cfg.RedisPoolSize,
		FilterTimeoutMs:  cfg.FilterTimeoutMs,
		StickyPinWorkers: cfg.MaxWorkers,
	})
	if err != nil {
		slog.Error("failed to connect to redis shards", "error", err)
		os.Exit(1)
	}

	channel := cfg.CampaignUpdateChannel
	if channel == "" {
		channel = "campaigns:update"
	}
	campaignRepo := ingestion.NewCampaignRepo(queries)
	sharder := ingestion.NewStaticSlotSharder(len(rdbs))
	if version, loadErr := ingestion.LoadActiveSlotMap(ctx, pool, sharder, len(rdbs)); loadErr != nil {
		slog.Warn("slot map load failed, using modulo fallback", "error", loadErr)
	} else {
		slog.Info("slot map loaded at startup", "version", version)
	}

	slotMapWatcher := ingestion.NewSlotMapWatcher(ingestion.SlotMapWatcherConfig{
		Pool:           pool,
		Sharder:        sharder,
		NumShards:      len(rdbs),
		PollInterval:   time.Duration(cfg.SlotMapPollIntervalMs) * time.Millisecond,
		BrokerURL:      cfg.Broker.URL,
		BrokerRedisURL: cfg.Broker.RedisURL,
		BrokerTopic:    cfg.SlotMapReloadTopic,
		BrokerTimeout:  time.Duration(cfg.Broker.TimeoutMs) * time.Millisecond,
	})
	go slotMapWatcher.Start(ctx)

	budgetWarmer := ingestion.NewBudgetCacheWarmer(rdbs, sharder)
	registry.SetBudgetWarmer(budgetWarmer)
	if warmed, err := budgetWarmer.WarmFromRegistry(ctx, registry); err != nil {
		slog.Error("initial budget cache warm failed", "error", err)
	} else {
		slog.Info("budget cache warmed", "keys_inserted", warmed)
	}

	registry.ConfigureStaleMode(time.Duration(cfg.RegistryStaleTTLSec) * time.Second)
	registry.StartWatch(ctx, rdbs[0], channel)

	if cfg.CampaignUpdateBrokerFallback && cfg.Broker.URL != "" {
		topic := cfg.CampaignUpdateBrokerTopic
		if topic == "" {
			topic = ingestion.DefaultCampaignUpdateBrokerTopic
		}
		cuWatcher := ingestion.NewCampaignUpdateWatcher(ingestion.CampaignUpdateWatcherConfig{
			Registry:       registry,
			BrokerURL:      cfg.Broker.URL,
			BrokerRedisURL: cfg.Broker.RedisURL,
			BrokerTopic:    topic,
			BrokerTimeout:  time.Duration(cfg.Broker.TimeoutMs) * time.Millisecond,
		})
		go cuWatcher.Start(ctx)
		slog.Info("campaign update broker fallback enabled", "topic", topic)
	}

	consentChannel := cfg.ConsentUpdateChannel
	if consentChannel == "" {
		consentChannel = ingestion.ConsentDefaultUpdateChannel
	}
	consentStore := ingestion.NewConsentStore(rdbs[0])
	consentStore.StartWatch(ctx, rdbs[0], consentChannel)

	var geoProvider ingestion.GeoProvider
	geoProvider, err = ingestion.NewMaxMindProvider(cfg.GeoIP.DBPath)
	if err != nil {
		if cfg.Env == "prod" || cfg.Env == "production" {
			slog.Error("FATAL: MaxMind DB load failed in production", "error", err)
			os.Exit(1)
		}
		slog.Warn("MaxMind DB load failed, using mock geo provider (development only)", "error", err)
		geoProvider = &ingestion.MockGeoProvider{}
	}
	defer geoProvider.Close()

	if mm, ok := geoProvider.(*ingestion.MaxMindProvider); ok {
		metrics.GeoProviderStatus.Set(1)
		watcherInterval := time.Duration(cfg.GeoIP.WatcherIntervalSec) * time.Second
		go ingestion.NewGeoIPWatcher(mm, cfg.GeoIP.DBPath, watcherInterval).Start(ctx)
		slog.Info("geoip hot-reload watcher started", "path", cfg.GeoIP.DBPath, "interval", watcherInterval)
	} else {
		metrics.GeoProviderStatus.Set(0)
	}

	geoFilter := ingestion.NewGeoFilter(geoProvider, registry)
	scheduleFilter := ingestion.NewScheduleFilter(registry)
	fraudFilter := ingestion.NewFraudFilter(geoProvider)

	settingsWatcher := ingestion.NewSettingsWatcher(rdbs, cfg)
	deviceFilter := ingestion.NewDeviceFilter(settingsWatcher)
	go settingsWatcher.Start(ctx, time.Second)

	breakerFilter := ingestion.NewEmergencyBreakerFilter(settingsWatcher)
	consentFilter := ingestion.NewConsentFilter(registry, consentStore)

	unifiedFilter := ingestion.NewUnifiedFilter(
		rdbs,
		sharder,
		registry,
		campaignRepo,
		0, // M9-03: IP rate limit enforced at XDP/nginx edge only
		time.Duration(cfg.RateLimitWindowMs)*time.Millisecond,
		time.Duration(cfg.DuplicateTTLSec)*time.Second,
		time.Duration(cfg.IdempotencyTTLHrs)*time.Hour,
		cfg.ClickAmount,
		cfg.ImpressionAmount,
		cfg.RedisStreamName,
		cfg.StreamMaxLen,
	)
	unifiedFilter.SetFilterEvalPinWorkers(cfg.MaxWorkers)
	unifiedFilter.SetShardBreakers(breakers)
	if err := unifiedFilter.PreloadScripts(ctx); err != nil {
		slog.Error("failed to preload redis lua scripts on all shards", "error", err)
		os.Exit(1)
	}
	unifiedFilter.SetTTCMin(time.Duration(cfg.TTCMinMs) * time.Millisecond)
	unifiedFilter.SetTTCFailClosed(cfg.TTCFailClosed)
	unifiedFilter.SetMetricsSampleMask(cfg.MetricsHistogramSampleMask)
	unifiedFilter.SetQuotaConfig(cfg.QuotaMode, cfg.QuotaChunkSize, cfg.QuotaRefillThresholdPct)
	unifiedFilter.SetLuaFastPathEnabled(cfg.LuaFastPathEnabled)
	unifiedFilter.SetRegionCode(cfg.RegionCode)
	unifiedFilter.SetPGFallbackAllowed(cfg.TrackerPGFallback)

	var localQuantaLedger *ingestion.LocalQuantaLedger
	var quotaRefillWorker *ingestion.QuotaRefillWorker
	var budgetDeltaPublisher *ingestion.BudgetDeltaPublisher
	var localQuantaFlusher *ingestion.LocalQuantaFlusher
	if cfg.LocalQuotaMode == "shadow" || cfg.LocalQuotaMode == "live" {
		localQuantaLedger = ingestion.NewLocalQuantaLedger()
		localQuantaStrict := ingestion.NewLocalQuantaStrict(cfg.QuotaStrictThresholdMicro, cfg.QuotaStrictExitMicro)
		chunkSize := cfg.QuotaChunkSize
		if chunkSize <= 0 {
			chunkSize = 5_000_000
		}
		quotaRefillWorker = ingestion.NewQuotaRefillWorker(
			localQuantaLedger,
			rdbs,
			sharder,
			ingestion.QuotaRefillConfig{
				BaseChunkMicro: chunkSize,
				ThresholdPct:   cfg.QuotaRefillThresholdPct,
				MaxPerShard:    cfg.LocalQuotaRefillMaxShard,
				FloorMicro:     cfg.QuotaAdaptiveFloorMicro,
				CeilingMicro:   cfg.QuotaAdaptiveCeilingMicro,
				StrictEnter:    cfg.QuotaStrictThresholdMicro,
			},
		)
		brokerRedisURL := cfg.Broker.RedisURL
		if brokerRedisURL == "" && len(cfg.RedisAddrs) > 0 {
			brokerRedisURL = "redis://" + cfg.RedisAddrs[0] + "/0"
		}
		budgetDeltaPublisher = ingestion.NewBudgetDeltaPublisher(ingestion.BudgetDeltaPublisherConfig{
			BrokerAddr: cfg.Broker.URL,
			RedisURL:   brokerRedisURL,
			Topic:      cfg.BudgetDeltaTopic,
			TrackerID:  cfg.RedisConsumerID,
			Timeout:    time.Duration(cfg.Broker.TimeoutMs) * time.Millisecond,
		})
		localQuantaFlusher = ingestion.NewLocalQuantaFlusher(localQuantaLedger, rdbs, sharder, budgetDeltaPublisher)
		quotaRefillWorker.SetStrictMode(localQuantaStrict, localQuantaFlusher)
		ingestion.SetRegistryQuantaFlushHook(func(id uuid.UUID) {
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer flushCancel()
			localQuantaFlusher.FlushLocalQuanta(flushCtx, id, ingestion.FlushReasonPause)
		})
		if cfg.Broker.URL != "" {
			recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 10*time.Second)
			deltas, recErr := ingestion.FetchRecoveryDeltas(recoveryCtx, ingestion.BrokerConsumerConfig{
				BrokerAddr: cfg.Broker.URL,
				RedisURL:   brokerRedisURL,
				Topic:      cfg.BudgetDeltaTopic,
				MaxBytes:   uint32(cfg.Broker.MaxBytes),
				Timeout:    time.Duration(cfg.Broker.TimeoutMs) * time.Millisecond,
			}, 0)
			recoveryCancel()
			if recErr != nil {
				slog.Warn("local quanta broker recovery skipped", "error", recErr)
			} else if len(deltas) > 0 {
				quotaRefillWorker.RecoverFromDeltas(deltas)
				slog.Info("local quanta ledger recovered from broker", "campaigns", len(deltas))
			}
		}
		unifiedFilter.SetLocalQuantaDeps(ingestion.LocalQuantaDeps{
			Ledger:    localQuantaLedger,
			Strict:    localQuantaStrict,
			Refill:    quotaRefillWorker,
			Publisher: budgetDeltaPublisher,
		})
		unifiedFilter.SetLocalQuantaMode(cfg.LocalQuotaMode)
		slog.Info("local quanta enabled",
			"mode", cfg.LocalQuotaMode,
			"chunk_size", chunkSize,
			"refill_threshold_pct", cfg.QuotaRefillThresholdPct,
			"strict_enter_micro", cfg.QuotaStrictThresholdMicro,
			"strict_exit_micro", cfg.QuotaStrictExitMicro,
		)
	}
	unifiedFilter.SetFilterSlowMs(cfg.FilterSlowMs)
	if cfg.TTCFailClosed {
		slog.Info("TTC fail-closed enabled: clicks without impression timestamp are rejected")
	}
	slog.Info("redis lua scripts preloaded", "shards", len(rdbs))

	creativeStore := ingestion.NewBrandCreativeStore(rdbs[0])
	licenseFilter := ingestion.NewLicenseFilter(registry)
	filterEngine := ingestion.NewFilterEngine(time.Duration(cfg.FilterTimeoutMs)*time.Millisecond, licenseFilter, breakerFilter, geoFilter, scheduleFilter, fraudFilter, deviceFilter, consentFilter, unifiedFilter)
	filterEngine.SetSettingsWatcher(settingsWatcher)

	var rtbCatalog *ingestion.RtbCatalog
	var rtbHybrid *ingestion.HybridBalancer
	var rtbReconcile *ingestion.RtbBudgetReconcileWorker
	rtbBudgetSync := ingestion.RtbBudgetSync{
		Authority: ingestion.BudgetAuthorityShadow,
		Redis:     rdbs,
		Sharder:   sharder,
	}
	if cfg.RtbEnabled() {
		rtb.SetMetricsEnabled(true)
		rtbStore := rtb.NewBudgetStore()
		rtbBudgetSync.Authority = ingestion.BudgetAuthorityFromConfig(cfg)
		rtbCatalog = ingestion.NewRtbCatalog(rtbStore, rtbBudgetSync.Authority)
		rtbHybrid = ingestion.NewHybridBalancer(len(rdbs), ingestion.HybridMaxRPSFromConfig(cfg))
		if cfg.RtbClearingMode == "first" {
			rtbCatalog.SetClearingMode(rtb.ClearingFirstPrice)
		}
		if cfg.RtbTargetingIndexEnabled() {
			rtbCatalog.Registry().SetTargetingIndexEnabled(true)
		}
		ingestion.StartRtbCatalogSync(ctx, registry, rtbCatalog, cfg, rtbHybrid, rtbBudgetSync, settingsWatcher, time.Duration(cfg.RegistrySyncIntervalMs)*time.Millisecond)
		if err := ingestion.ReloadRtbDeals(ctx, queries, rtbCatalog); err != nil {
			slog.Warn("initial rtb deals load failed", "error", err)
		} else {
			slog.Info("rtb deals loaded", "count", rtbCatalog.DealCount())
		}
		ingestion.StartRtbCatalogReloadWatch(ctx, queries, rdbs[0], ingestion.RtbCatalogReloadChannel(cfg), registry, rtbCatalog, cfg, rtbHybrid, rtbBudgetSync, settingsWatcher)
		dealFloorCache := ingestion.NewDealFloorCache(rdbs[0])
		rtbCatalog.SetDealFloors(dealFloorCache)
		ingestion.StartDealFloorRefresh(ctx, dealFloorCache, rtbCatalog, time.Duration(cfg.DealFloorRefreshIntervalMs)*time.Millisecond)
		if allow, err := ingestion.LoadSupplyChainAllowlist(ctx, queries); err == nil {
			rtbCatalog.SetSupplyChainAllowlist(allow)
		} else {
			slog.Warn("initial supply chain allowlist load failed", "error", err)
		}
		var rtbBudgetMirror *ingestion.RtbBudgetMirrorWriter
		if rtbBudgetSync.Authority == ingestion.BudgetAuthorityRTB {
			rtbBudgetMirror = ingestion.NewRtbBudgetMirrorWriter(rtbCatalog, registry, rdbs, sharder)
			defer func() {
				if rtbBudgetMirror != nil {
					rtbBudgetMirror.Close()
				}
			}()
		}
		_ = ingestion.NewRtbAuthorityController(cfg, settingsWatcher, unifiedFilter, rtbCatalog, &rtbBudgetSync)
		rtbReconcile = ingestion.NewRtbBudgetReconcileWorker(
			ingestion.RtbBudgetReconcileConfig{
				Interval:            time.Duration(cfg.RtbReconcileIntervalMs) * time.Millisecond,
				DivergenceThreshold: cfg.RtbBudgetDivergenceMicro,
				SampleSize:          cfg.RtbReconcileSampleSize,
			},
			registry,
			rtbCatalog,
			rdbs,
			sharder,
		)
		rtbReconcile.Start(ctx)
		if snapPath := cfg.RtbSnapshotPath; snapPath != "" {
			if err := rtbCatalog.Registry().StartPersistence(ctx, snapPath, time.Minute); err != nil {
				slog.Warn("rtb snapshot persistence disabled", "error", err)
			} else {
				slog.Info("rtb snapshot persistence enabled", "path", snapPath)
			}
		}
		slog.Info("rtb catalog enabled",
			"mode", cfg.RtbMode,
			"budget_authority", cfg.RtbBudgetAuthority,
			"targeting_index", cfg.RtbTargetingIndexEnabled(),
		)
	}

	gnetHandler := ingestion.NewAdsPacketHandler(cfg, registry, filterEngine, pool, rdbs, sharder, cfg.FraudStreamName, creativeStore)
	ingestion.StartFraudBackpressureWatcher(ctx, ingestion.FraudBackpressureConfig{
		Rdbs:        rdbs,
		Writer:      gnetHandler.FraudWriter(),
		Stream:      cfg.FraudStreamName,
		EventStream: cfg.RedisStreamName,
		Group:       cfg.RedisGroupName,
		LagSec:      cfg.FraudConsumerLagSec,
		Interval:    2 * time.Second,
	})
	if udpCtrl := ingestion.NewUDPControlFromConfig(cfg, len(rdbs)); udpCtrl != nil {
		if err := udpCtrl.Start(ctx); err != nil {
			slog.Error("udp control start failed", "error", err)
			os.Exit(1)
		}
		defer udpCtrl.Close()
		gnetHandler.SetUDPControl(udpCtrl)
		slog.Info("udp ingress control enabled", "fail_closed", cfg.UDPFailClosed)
	}
	if cfg.TCPControlEnabled {
		tcpClient := ingestion.NewTCPControlClient(ingestion.TCPControlClientConfig{
			Enabled:   true,
			Secret:    []byte(cfg.TCPControlHMACSecret),
			TrackerID: cfg.UDPTrackerID,
			MgmtAddr:  cfg.TCPMgmtAddr,
			Sharder:   sharder,
		})
		go func() {
			ticker := time.NewTicker(time.Duration(cfg.SlotMapPollIntervalMs) * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := tcpClient.RequestSnapshot(ctx); err != nil {
						slog.Debug("tcp routing snapshot pull", "error", err)
					}
				}
			}
		}()
		slog.Info("tcp routing snapshot client enabled", "mgmt", cfg.TCPMgmtAddr)
	}
	gnetHandler.ConfigureIngestGeo(geoProvider)
	if rtbCatalog != nil {
		gnetHandler.ConfigureRtb(rtbCatalog, geoProvider, unifiedFilter, settingsWatcher)
	}
	gnetHandler.SetLogger(appLogger)
	gnetHandler.StartHealthProbe(ctx)

	workerPool := ingestion.NewPinnedWorkerPool(cfg.MaxWorkers, 8192)
	gnetHandler.SetWorkerPool(workerPool)

	slog.Info("starting ad-event-tracker via gnet", "port", cfg.ServerPort)

	go func() {
		err := gnet.Run(gnetHandler, "tcp://:"+cfg.ServerPort,
			gnet.WithMulticore(true),
			gnet.WithReusePort(true),
			gnet.WithTCPNoDelay(gnet.TCPNoDelay),
			gnet.WithNumEventLoop(2),
			gnet.WithLockOSThread(false),
		)
		if err != nil {
			slog.Error("gnet server failed", "error", err)
			os.Exit(1)
		}
	}()

	metricsMux := http.NewServeMux()
	live := &health.Liveness{}
	ready := &health.ReadinessProbe{}
	ready.StartBackground(ctx, 2*time.Second, func(probeCtx context.Context) bool {
		return gnetHandler.Ready()
	})
	health.Register(metricsMux, live, ready)
	metricsMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ready.ServeReadyz(w, r)
	})
	metricsMux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gnetHandler.FlushLatency()
		promhttp.Handler().ServeHTTP(w, r)
	}))
	metricsSrv := &http.Server{
		Addr:              ":" + cfg.MetricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics sidecar server failed", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	sig := <-stop
	slog.Info("received shutdown signal", "signal", sig.String())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	cancel()

	if rtbReconcile != nil {
		rtbReconcile.Close()
		reconcileWaitCtx, reconcileWaitCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.WaitTimeoutMs)*time.Millisecond)
		if err := rtbReconcile.Wait(reconcileWaitCtx); err != nil {
			slog.Warn("rtb budget reconcile wait failed", "error", err)
		}
		reconcileWaitCancel()
	}

	if err := gnetHandler.Stop(shutdownCtx); err != nil {
		slog.Error("gnet server shutdown failed", "error", err)
	}

	metricsShutdownCtx, metricsCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.WaitTimeoutMs)*time.Millisecond)
	if err := metricsSrv.Shutdown(metricsShutdownCtx); err != nil {
		slog.Error("metrics server shutdown failed", "error", err)
	}
	metricsCancel()

	workerPool.Shutdown()

	// M14-14: flush unused RAM quanta before closing refill/publisher/pins.
	if localQuantaFlusher != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
		n := localQuantaFlusher.FlushAll(flushCtx)
		flushCancel()
		if n > 0 {
			slog.Info("local quanta flushed on shutdown", "campaigns", n)
		}
		ingestion.SetRegistryQuantaFlushHook(nil)
	}

	if quotaRefillWorker != nil {
		quotaRefillWorker.Close()
	}
	if budgetDeltaPublisher != nil {
		budgetDeltaPublisher.Close()
	}

	registryWaitCtx, registryWaitCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.WaitTimeoutMs)*time.Millisecond)
	defer registryWaitCancel()
	if err := registry.Wait(registryWaitCtx); err != nil {
		slog.Error("registry wait failed", "error", err)
	}

	unifiedFilter.CloseFilterEvalPins()

	for i, rdb := range rdbs {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis shard", "shard", i, "error", err)
		}
	}
	slog.Info("ad-event-tracker shutdown complete")
}

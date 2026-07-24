// Command processor wires per-shard Redis stream consumers into Postgres, ClickHouse, and fraud pipelines.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"espx/internal/clickhouse/migrate"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/dedup"
	"espx/internal/fraudscoring"
	"espx/internal/health"
	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/licensing"
	"espx/internal/management"
	"espx/internal/metrics"
	"espx/pkg/logger"
	"fmt"
	"strconv"

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

	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
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

	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	syncCtx, syncCancel := context.WithCancel(context.Background())
	defer syncCancel()

	pool, err := database.Connect(ctx, string(cfg.DBDSN), cfg.DBProcessorMaxConns, cfg.DBMinConns)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	procPgGate := ingestion.NewProcessorPgGate(cfg.ProcessorPGGateSlots, cfg.DBProcessorMaxConns)
	procChGate := ingestion.NewProcessorChGate(cfg.ProcessorCHGateSlots, cfg.CHMaxConns)

	queries := db.New(pool)
	partManager := database.NewPartitionManager(pool, cfg.LogRetentionDays, cfg.PartitionPreCreateDays)
	partManager.StartBackground(ctx)

	if cfg.GeoIP.UpdaterEnabled {
		updater := ingestion.NewGeoIPUpdater(ingestion.GeoIPUpdaterConfig{
			DBPath:         cfg.GeoIP.DBPath,
			StagingPath:    cfg.GeoIP.StagingPath,
			EditionID:      cfg.GeoIP.EditionID,
			LicenseKey:     cfg.GeoIP.LicenseKey,
			UpdateInterval: time.Duration(cfg.GeoIP.UpdateIntervalHours) * time.Hour,
		})
		go updater.Start(ctx)
		slog.Info("geoip updater started",
			"path", cfg.GeoIP.DBPath,
			"interval_hours", cfg.GeoIP.UpdateIntervalHours,
		)
	}

	chConn, err := database.ConnectClickHouse(ctx, string(cfg.CHDSN))
	if err != nil {
		slog.Error("failed to connect to clickhouse", "error", err)
		os.Exit(1)
	}
	defer chConn.Close()

	if err := migrate.ApplyClickHouseMigrations(ctx, chConn); err != nil {
		slog.Error("failed to apply clickhouse migrations", "error", err)
		os.Exit(1)
	}

	notifierClient, notifierErr := management.NewNotifierClient(cfg)
	if notifierErr != nil {
		slog.Warn("notifier client initialization failed", "error", notifierErr)
	}
	if notifierClient != nil {
		defer notifierClient.Close()
	}
	opsAlerter := management.NewOpsAlerter(notifierClient, cfg)
	var onEmergencyDrop database.EmergencyDropAlerter
	if opsAlerter != nil && cfg.CHEmergencyDropPercent > 0 {
		threshold := cfg.CHEmergencyDropPercent
		onEmergencyDrop = func(table, partition string, diskPct float64) {
			opsAlerter.AlertCHEmergencyDrop(table, partition, diskPct, threshold)
		}
	}
	chJanitor := database.NewCHPartitionJanitor(chConn, database.CHJanitorOptions{
		RetentionDays:            cfg.CHRawRetentionDays,
		EmergencyDropPercent:     cfg.CHEmergencyDropPercent,
		RecompressPartsThreshold: cfg.CHRecompressPartsThreshold,
		OffPeakStartHourUTC:      cfg.CHRecompressOffPeakStartUTC,
		OffPeakEndHourUTC:        cfg.CHRecompressOffPeakEndUTC,
		OnEmergencyDrop:          onEmergencyDrop,
	})
	chJanitor.StartBackground(ctx, 24*time.Hour)

	var rdbs []redis.UniversalClient
	rdbs, _, err = database.ConnectRedisShards(ctx, cfg, database.RedisShardOptions{
		PoolSize: cfg.RedisPoolSize,
	})
	if err != nil {
		slog.Error("failed to connect to redis shards", "error", err)
		os.Exit(1)
	}

	pgStore := ingestion.NewPostgresStoreWithGate(queries, time.Duration(cfg.WriteTimeoutMs)*time.Millisecond, procPgGate)
	spoolCfg := ingestion.CHCfgFromConfig(cfg.CHSpoolSegmentMB, cfg.CHSpoolMaxSegments)
	chStore := ingestion.NewClickHouseStore(chConn, time.Duration(cfg.WriteTimeoutMs)*time.Millisecond, cfg.CHSpoolDir, spoolCfg, procChGate)
	if err := chStore.RecoverSpool(ctx); err != nil {
		slog.Error("failed to recover clickhouse spool", "error", err)
		os.Exit(1)
	}

	var fraudScorer fraudscoring.Scorer
	if cfg.FraudScoringEnabled() {
		snap, snapErr := licensing.LoadDeploymentSnapshot(ctx, pool)
		if snapErr == nil && snap.ModuleAllowed(func(f licensing.FeatureSet) bool { return f.MlFraudBoostEnabled() }) {
			var err error
			fraudScorer, err = fraudscoring.NewLGBMScorer(cfg.FraudScoring.ModelPath)
			if err != nil {
				slog.Error("failed to initialize fraud scorer for processor micro-batching", "error", err, "path", cfg.FraudScoring.ModelPath)
				os.Exit(1)
			}
			slog.Info("initialized fraud scorer for processor micro-batching", "path", cfg.FraudScoring.ModelPath)
		} else {
			slog.Info("ml_fraud_boost not licensed; processor fraud micro-batching disabled")
		}
	}

	campaignRepo := ingestion.NewCampaignRepoWithDB(pool, queries)
	campaignRepo.ConfigureAuditLedgerFlush(cfg.AuditLedgerFlushSampleMask)
	customerRepo := ingestion.NewCustomerRepoWithDB(pool, queries)
	dedupAdapter := dedup.NewAdapter(pool, cfg.RegionCode, dedup.LoadRoutingEpoch(ctx, pool))

	var pgConsumers []*ingestion.StreamConsumer
	var chConsumers []*ingestion.StreamConsumer
	var brokerConsumers []*ingestion.BrokerStreamConsumer
	var brokerReconcile *ingestion.BrokerReconcileWorker
	var budgetDeltaConsumer *ingestion.BudgetDeltaConsumer
	var syncWorkers []*ingestion.SyncWorker

	for i, rdb := range rdbs {
		shardID := fmt.Sprintf("shard_%d", i)

		sw := ingestion.NewSyncWorker(rdb, campaignRepo, customerRepo, time.Duration(cfg.BudgetSyncIntervalMs)*time.Millisecond, time.Duration(cfg.LedgerBatchFlushMs)*time.Millisecond, procPgGate, 0)
		sw.SetDedupAdapter(dedupAdapter)
		sw.ConfigureBudgetContention(
			ingestion.BudgetLockTTLSeconds(cfg.LedgerBatchFlushMs, cfg.BudgetSyncIntervalMs),
			cfg.QuotaStrictThresholdMicro,
		)
		syncWorkers = append(syncWorkers, sw)
		sw.Start(syncCtx)

		pc := ingestion.NewStreamConsumer(
			pgStore,
			rdb,
			cfg.RedisStreamName,
			cfg.RedisGroupName+"_pg",
			cfg.RedisConsumerID+"_"+shardID,
			cfg.EventBatchSize,
			cfg.ProcessorPGStreamWorkers(),
			time.Duration(cfg.EventFlushMs)*time.Millisecond,
			time.Duration(cfg.WriteTimeoutMs)*time.Millisecond,
			time.Duration(cfg.RetryInitialWaitMs)*time.Millisecond,
			time.Duration(cfg.RetryMaxWaitMs)*time.Millisecond,
			cfg.MaxRetries,
			time.Duration(cfg.StreamMinIdleMs)*time.Millisecond,
			time.Duration(cfg.Lifecycle.DrainTimeoutMs)*time.Millisecond,
		)
		pc.SetLogger(appLogger)
		pc.SetAuditLogSampleMask(cfg.AuditLogSampleMask)
		pgConsumers = append(pgConsumers, pc)
		pc.Start(consumerCtx)

		cc := ingestion.NewStreamConsumer(
			chStore,
			rdb,
			cfg.RedisStreamName,
			cfg.RedisGroupName+"_ch",
			cfg.RedisConsumerID+"_"+shardID,
			cfg.CHBatchSize,
			cfg.ProcessorCHStreamWorkers(),
			time.Duration(cfg.CHFlushIntervalMs)*time.Millisecond,
			time.Duration(cfg.WriteTimeoutMs)*time.Millisecond,
			time.Duration(cfg.RetryInitialWaitMs)*time.Millisecond,
			time.Duration(cfg.RetryMaxWaitMs)*time.Millisecond,
			cfg.MaxRetries,
			time.Duration(cfg.StreamMinIdleMs)*time.Millisecond,
			time.Duration(cfg.Lifecycle.DrainTimeoutMs)*time.Millisecond,
		)
		cc.SetLogger(appLogger)
		cc.SetAuditLogSampleMask(cfg.AuditLogSampleMask)

		if fraudScorer != nil {
			mb := fraudscoring.NewMicroBatcher(rdb, fraudScorer)
			go mb.Start(consumerCtx)
			cc.SetOnMessageProcessed(mb.Enqueue)
		}

		chConsumers = append(chConsumers, cc)
		cc.Start(consumerCtx)

		fc := ingestion.NewStreamConsumer(
			chStore,
			rdb,
			cfg.FraudStreamName,
			cfg.RedisGroupName+"_fraud",
			cfg.RedisConsumerID+"_fraud_"+shardID,
			cfg.CHBatchSize,
			cfg.ProcessorCHStreamWorkers(),
			time.Duration(cfg.CHFlushIntervalMs)*time.Millisecond,
			time.Duration(cfg.WriteTimeoutMs)*time.Millisecond,
			time.Duration(cfg.RetryInitialWaitMs)*time.Millisecond,
			time.Duration(cfg.RetryMaxWaitMs)*time.Millisecond,
			cfg.MaxRetries,
			time.Duration(cfg.StreamMinIdleMs)*time.Millisecond,
			time.Duration(cfg.Lifecycle.DrainTimeoutMs)*time.Millisecond,
		)
		fc.SetLogger(appLogger)
		fc.SetAuditLogSampleMask(cfg.AuditLogSampleMask)
		chConsumers = append(chConsumers, fc)
		fc.Start(consumerCtx)
	}

	ingestion.StartFraudLagPublisher(
		ctx,
		rdbs,
		cfg.FraudStreamName,
		cfg.RedisGroupName+"_fraud",
		cfg.FraudConsumerLagSec,
		2*time.Second,
	)

	if cfg.BrokerEnabled() {
		brokerRedisURL := cfg.Broker.RedisURL
		if brokerRedisURL == "" && len(cfg.RedisAddrs) > 0 {
			brokerRedisURL = "redis://" + cfg.RedisAddrs[0] + "/0"
		}
		brokerBase := ingestion.BrokerConsumerConfig{
			BrokerAddr: cfg.Broker.URL,
			RedisURL:   brokerRedisURL,
			Topic:      cfg.Broker.Topic,
			BatchSize:  cfg.EventBatchSize,
			FlushInt:   time.Duration(cfg.EventFlushMs) * time.Millisecond,
			MaxBytes:   uint32(cfg.Broker.MaxBytes),
			Timeout:    time.Duration(cfg.Broker.TimeoutMs) * time.Millisecond,
			ShadowMode: cfg.Broker.ShadowMode,
		}
		partCount := cfg.Broker.PartitionCount
		if partCount <= 0 {
			partCount = 1
		}
		writeTimeout := time.Duration(cfg.WriteTimeoutMs) * time.Millisecond
		retryInit := time.Duration(cfg.RetryInitialWaitMs) * time.Millisecond
		retryMax := time.Duration(cfg.RetryMaxWaitMs) * time.Millisecond

		for p := 0; p < partCount; p++ {
			pgBrokerCfg := brokerBase
			pgBrokerCfg.Partition = uint16(p)
			pgBrokerCfg.Group = cfg.RedisGroupName + "_pg_broker"
			pgBroker := ingestion.NewBrokerStreamConsumer(pgStore, pgBrokerCfg, writeTimeout, retryInit, retryMax, cfg.MaxRetries)
			pgBroker.SetDedupAdapter(dedupAdapter)
			pgBroker.SetLogger(appLogger)
			brokerConsumers = append(brokerConsumers, pgBroker)
			pgBroker.Start(consumerCtx)

			chBrokerCfg := brokerBase
			chBrokerCfg.Partition = uint16(p)
			chBrokerCfg.Group = cfg.RedisGroupName + "_ch_broker"
			chBrokerCfg.BatchSize = cfg.CHBatchSize
			chBrokerCfg.FlushInt = time.Duration(cfg.CHFlushIntervalMs) * time.Millisecond
			chBroker := ingestion.NewBrokerStreamConsumer(chStore, chBrokerCfg, writeTimeout, retryInit, retryMax, cfg.MaxRetries)
			chBroker.SetLogger(appLogger)
			brokerConsumers = append(brokerConsumers, chBroker)
			chBroker.Start(consumerCtx)
		}

		if len(rdbs) > 0 {
			brokerReconcile = ingestion.NewBrokerReconcileWorker(ingestion.BrokerReconcileConfig{
				BrokerAddr:          cfg.Broker.URL,
				BrokerRedis:         brokerRedisURL,
				Topic:               cfg.Broker.Topic,
				PartitionCount:      partCount,
				BrokerGroup:         cfg.RedisGroupName + "_pg_broker",
				StreamName:          cfg.RedisStreamName,
				Interval:            time.Duration(cfg.Broker.ReconcileIntervalMs) * time.Millisecond,
				DivergenceThreshold: cfg.Broker.DivergenceThreshold,
			}, rdbs)
			brokerReconcile.Start(consumerCtx)
		}

		slog.Info("broker ingest bridge enabled",
			"broker", cfg.Broker.URL,
			"topic", cfg.Broker.Topic,
			"partitions", partCount,
			"shadow_mode", cfg.Broker.ShadowMode,
			"pg_group", cfg.RedisGroupName+"_pg_broker",
			"ch_group", cfg.RedisGroupName+"_ch_broker",
		)
	}

	if cfg.BrokerEnabled() && (cfg.LocalQuotaMode == "shadow" || cfg.LocalQuotaMode == "live") {
		brokerRedisURL := cfg.Broker.RedisURL
		if brokerRedisURL == "" && len(cfg.RedisAddrs) > 0 {
			brokerRedisURL = "redis://" + cfg.RedisAddrs[0] + "/0"
		}
		budgetDeltaConsumer = ingestion.NewBudgetDeltaConsumer(
			ingestion.NewBudgetDeltaAggregator(),
			ingestion.BrokerConsumerConfig{
				BrokerAddr: cfg.Broker.URL,
				RedisURL:   brokerRedisURL,
				Topic:      cfg.BudgetDeltaTopic,
				Group:      cfg.RedisGroupName + "_budget_delta",
				MaxBytes:   uint32(cfg.Broker.MaxBytes),
				Timeout:    time.Duration(cfg.Broker.TimeoutMs) * time.Millisecond,
			},
		)
		budgetDeltaConsumer.Start(consumerCtx)
		slog.Info("budget delta consumer enabled", "topic", cfg.BudgetDeltaTopic)
	}

	slog.Info("starting ad-event-processor worker",
		"stream", cfg.RedisStreamName,
		"pg_group", cfg.RedisGroupName+"_pg",
		"ch_group", cfg.RedisGroupName+"_ch",
		"port", cfg.ProcessorPort,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	live := &health.Liveness{}
	ready := &health.ReadinessProbe{}
	ready.StartBackground(ctx, 2*time.Second, func(probeCtx context.Context) bool {
		if err := pool.Ping(probeCtx); err != nil {
			return false
		}
		if err := chConn.Ping(probeCtx); err != nil {
			return false
		}
		for i, rdb := range rdbs {
			if err := rdb.Ping(probeCtx).Err(); err != nil {
				return false
			}
			if cfg.RedisStreamName != "" {
				if n, err := rdb.XLen(probeCtx, cfg.RedisStreamName).Result(); err == nil {
					metrics.ProcessorStreamXLen.WithLabelValues(strconv.Itoa(i)).Set(float64(n))
				}
			}
		}
		if spool := chStore.Spool(); spool != nil {
			seg := spool.SegmentCount()
			metrics.CHSpoolSegments.Set(float64(seg))
			if seg > cfg.CHSpoolMaxSegments {
				return false
			}
		}
		if cfg.ProcessorStreamLagMaxSec > 0 && ingestion.ProcessorStreamLagSec() > int64(cfg.ProcessorStreamLagMaxSec) {
			return false
		}
		return true
	})
	health.Register(mux, live, ready)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ready.ServeReadyz(w, r)
	})

	server := &http.Server{
		Addr:    ":" + cfg.ProcessorPort,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("processor http server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down processor")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.ShutdownTimeoutMs)*time.Millisecond)
	defer shutdownCancel()

	consumerCancel()

	for _, bc := range brokerConsumers {
		bc.Close()
	}
	if budgetDeltaConsumer != nil {
		budgetDeltaConsumer.Close()
	}
	if brokerReconcile != nil {
		brokerReconcile.Close()
	}

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("processor server shutdown failed", "error", err)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Lifecycle.WaitTimeoutMs)*time.Millisecond)
	defer waitCancel()

	for _, bc := range brokerConsumers {
		if err := bc.Wait(waitCtx); err != nil {
			slog.Error("broker consumer wait failed", "error", err)
		}
	}
	if brokerReconcile != nil {
		if err := brokerReconcile.Wait(waitCtx); err != nil {
			slog.Error("broker reconcile wait failed", "error", err)
		}
	}

	for _, pc := range pgConsumers {
		pc.Close()
		if err := pc.Wait(waitCtx); err != nil {
			slog.Error("pg consumer wait failed", "error", err)
		}
	}
	pgStore.Close()

	for _, cc := range chConsumers {
		cc.Close()
		if err := cc.Wait(waitCtx); err != nil {
			slog.Error("ch consumer wait failed", "error", err)
		}
	}
	chStore.Close()

	syncCancel()
	for i, sw := range syncWorkers {
		if err := sw.Wait(waitCtx); err != nil {
			slog.Error("sync worker wait failed", "shard", i, "error", err)
		}
	}

	if err := partManager.Wait(waitCtx); err != nil {
		slog.Error("partition manager wait failed", "error", err)
	}
	chJanitor.Wait()

	cancel()

	for i, rdb := range rdbs {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis shard", "shard", i, "error", err)
		}
	}
	slog.Info("processor shutdown complete")
}

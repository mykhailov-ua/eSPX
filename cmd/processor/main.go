// Command processor drains Redis ad-event streams into Postgres, ClickHouse, and fraud analytics.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"
	"espx/pkg/logger"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// main runs stream consumers per Redis shard because ad events fan out to Postgres, ClickHouse, and fraud pipelines independently.
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

	queries := db.New(pool)
	partManager := database.NewPartitionManager(pool, cfg.LogRetentionDays, cfg.PartitionPreCreateDays)
	partManager.StartBackground(ctx)

	if cfg.GeoIP.UpdaterEnabled {
		updater := ads.NewGeoIPUpdater(ads.GeoIPUpdaterConfig{
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

	var rdbs []redis.UniversalClient
	rdbs, err = database.ConnectRedisShards(ctx, cfg, database.RedisShardOptions{
		PoolSize: cfg.RedisPoolSize,
	})
	if err != nil {
		slog.Error("failed to connect to redis shards", "error", err)
		os.Exit(1)
	}

	pgStore := ads.NewPostgresStore(queries, time.Duration(cfg.WriteTimeoutMs)*time.Millisecond)
	chStore := ads.NewClickHouseStore(chConn, time.Duration(cfg.WriteTimeoutMs)*time.Millisecond)

	campaignRepo := ads.NewCampaignRepo(queries)
	customerRepo := ads.NewCustomerRepo(queries)

	var pgConsumers []*ads.StreamConsumer
	var chConsumers []*ads.StreamConsumer
	var brokerConsumers []*ads.BrokerStreamConsumer
	var brokerReconcile *ads.BrokerReconcileWorker
	var syncWorkers []*ads.SyncWorker

	for i, rdb := range rdbs {
		shardID := fmt.Sprintf("shard_%d", i)

		sw := ads.NewSyncWorker(rdb, campaignRepo, customerRepo, time.Duration(cfg.BudgetSyncIntervalMs)*time.Millisecond)
		syncWorkers = append(syncWorkers, sw)
		sw.Start(syncCtx)

		pc := ads.NewStreamConsumer(
			pgStore,
			rdb,
			cfg.RedisStreamName,
			cfg.RedisGroupName+"_pg",
			cfg.RedisConsumerID+"_"+shardID,
			cfg.EventBatchSize,
			cfg.MaxWorkers,
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

		cc := ads.NewStreamConsumer(
			chStore,
			rdb,
			cfg.RedisStreamName,
			cfg.RedisGroupName+"_ch",
			cfg.RedisConsumerID+"_"+shardID,
			cfg.CHBatchSize,
			cfg.CHMaxWorkers,
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
		chConsumers = append(chConsumers, cc)
		cc.Start(consumerCtx)

		fc := ads.NewStreamConsumer(
			chStore,
			rdb,
			cfg.FraudStreamName,
			cfg.RedisGroupName+"_fraud",
			cfg.RedisConsumerID+"_fraud_"+shardID,
			cfg.CHBatchSize,
			cfg.CHMaxWorkers,
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

	if cfg.BrokerEnabled() {
		brokerRedisURL := cfg.Broker.RedisURL
		if brokerRedisURL == "" && len(cfg.RedisAddrs) > 0 {
			brokerRedisURL = "redis://" + cfg.RedisAddrs[0] + "/0"
		}
		brokerBase := ads.BrokerConsumerConfig{
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
			pgBroker := ads.NewBrokerStreamConsumer(pgStore, pgBrokerCfg, writeTimeout, retryInit, retryMax, cfg.MaxRetries)
			pgBroker.SetLogger(appLogger)
			brokerConsumers = append(brokerConsumers, pgBroker)
			pgBroker.Start(consumerCtx)

			chBrokerCfg := brokerBase
			chBrokerCfg.Partition = uint16(p)
			chBrokerCfg.Group = cfg.RedisGroupName + "_ch_broker"
			chBrokerCfg.BatchSize = cfg.CHBatchSize
			chBrokerCfg.FlushInt = time.Duration(cfg.CHFlushIntervalMs) * time.Millisecond
			chBroker := ads.NewBrokerStreamConsumer(chStore, chBrokerCfg, writeTimeout, retryInit, retryMax, cfg.MaxRetries)
			chBroker.SetLogger(appLogger)
			brokerConsumers = append(brokerConsumers, chBroker)
			chBroker.Start(consumerCtx)
		}

		if len(rdbs) > 0 {
			brokerReconcile = ads.NewBrokerReconcileWorker(ads.BrokerReconcileConfig{
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

	slog.Info("starting ad-event-processor worker",
		"stream", cfg.RedisStreamName,
		"pg_group", cfg.RedisGroupName+"_pg",
		"ch_group", cfg.RedisGroupName+"_ch",
		"port", cfg.ProcessorPort,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			slog.Error("processor health check failed: postgres", "error", err)
			http.Error(w, "postgres unreachable", http.StatusServiceUnavailable)
			return
		}

		if err := chConn.Ping(ctx); err != nil {
			slog.Error("processor health check failed: clickhouse", "error", err)
			http.Error(w, "clickhouse unreachable", http.StatusServiceUnavailable)
			return
		}

		for i, rdb := range rdbs {
			if err := rdb.Ping(ctx).Err(); err != nil {
				slog.Error("processor health check failed: redis shard", "shard", i, "error", err)
				http.Error(w, "redis shard unreachable", http.StatusServiceUnavailable)
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
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

	cancel()

	for i, rdb := range rdbs {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close redis shard", "shard", i, "error", err)
		}
	}
	slog.Info("processor shutdown complete")
}

// Package metrics registers Prometheus collectors shared across ingestion, filter, management, and broker services.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTP ingress metrics exist so Grafana can alert on error rate and latency SLO breaches.
	HttpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_http_requests_total",
		Help: "Total number of HTTP requests by status code",
	}, []string{"method", "path", "status"})

	HttpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_http_request_duration_seconds",
		Help:    "Latency of HTTP requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	// Event ingestion metrics exist so ops can detect silent data loss at the Redis Streams boundary before downstream consumers starve.
	EventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_events_processed_total",
		Help: "Total number of events successfully accepted into Redis Streams",
	})

	EventsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_events_dropped_total",
		Help: "Total number of events dropped due to Redis ingestion failure",
	})

	// Filter block metrics exist so on-call can spot fraud spikes, policy misconfiguration, or abnormal rejection mix by reason.
	FilterBlockedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_filter_blocked_total",
		Help: "Total number of events blocked by filters",
	}, []string{"reason"})

	// Database write metrics exist so persistence slowdowns and write failures page before the processor backlog grows.
	DbWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_db_write_duration_seconds",
		Help:    "Duration of database batch write operations",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"type"})

	DbWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_db_write_errors_total",
		Help: "Total number of database write errors",
	}, []string{"type"})

	// Circuit breaker metrics exist so alerts fire when dependency protection opens and the hot path starts shedding load.
	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_circuit_breaker_state",
		Help: "Current state of the circuit breaker (0=closed, 1=open, 2=half-open)",
	}, []string{"group"})

	RedisBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_redis_breaker_state",
		Help: "Current state of the Redis shard circuit breaker (0=closed, 1=open, 2=half-open)",
	}, []string{"shard"})

	// DLQ depth exists so unreplayable or stuck events trigger investigation before they accumulate without visibility.
	DlqSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_dlq_size_total",
		Help: "Current number of events in the Dead Letter Queue",
	})

	// Management business metrics exist so finance and ops dashboards can track revenue flow and live campaign inventory.
	CommissionsCollectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_management_commissions_total",
		Help: "Total amount of commissions collected from campaign cancellations",
	})

	BalanceTopupsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_management_topups_total",
		Help: "Total amount of customer balance top-ups",
	}, []string{"currency"})

	ActiveCampaigns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_management_active_campaigns_count",
		Help: "Current number of active campaigns in the system",
	})

	// Reconciliation metrics exist so billing integrity alerts fire when Postgres and ClickHouse spend diverge or auto-corrections fail.
	DataDriftRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_reconciliation_drift_ratio",
		Help: "Ratio of discrepancy between Postgres and ClickHouse spend",
	}, []string{"campaign_id"})

	ReconRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_reconciliation_runs_total",
		Help: "Total number of completed reconciliation runs",
	}, []string{"status"})

	ReconDiscrepanciesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_reconciliation_discrepancies_total",
		Help: "Total number of campaign discrepancies found",
	})

	ReconTotalDelta = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_reconciliation_total_delta_micro_units",
		Help: "Absolute net discrepancy corrected by reconciliation in micro units",
	})

	ReconAdjustmentErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_reconciliation_adjustment_errors_total",
		Help: "Total number of errors during automated reconciliation corrections",
	})

	// gnet hot-path metrics exist so capacity alerts catch connection saturation, parse failures, and worker pool overload before requests are dropped.
	GnetPacketsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_packets_received_total",
		Help: "Total number of network packets received",
	})
	GnetPacketsSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_packets_sent_total",
		Help: "Total number of network packets sent",
	})
	GnetActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_gnet_active_connections",
		Help: "Current number of active TCP connections",
	})
	GnetEventLoopWorkDuration = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_event_loop_work_duration_seconds_total",
		Help: "Total execution time spent doing active processing in gnet event loops",
	})
	GnetBytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_bytes_received_total",
		Help: "Total number of bytes received via gnet",
	})
	GnetBytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_gnet_bytes_sent_total",
		Help: "Total number of bytes sent via gnet",
	})
	HttpParseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_http_parse_errors_total",
		Help: "Total number of HTTP/1.1 parsing errors",
	}, []string{"error_type"})
	WorkerPoolRejectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_worker_pool_reject_total",
		Help: "Requests rejected because pinned worker pool queue is full",
	})

	// Async side-effect drop metrics exist so audit and fraud pipelines surface ring-buffer overflow before compliance gaps go unnoticed.
	HandlerLogDropTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_handler_log_drop_total",
		Help: "Accepted events whose audit log write was dropped (logger ring full)",
	})
	FraudStreamDropTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_fraud_stream_drop_total",
		Help: "Fraud reject events dropped because the async fraud stream ring is full",
	})

	// Filter engine metrics exist so delivery health dashboards expose pass/block mix, dependency blips, and geo lookup tail latency.
	FilterThroughput = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_filter_throughput_total",
		Help: "Total throughput through the filter engine",
	}, []string{"format"})
	FilterDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_filter_decisions_total",
		Help: "Filter decisions made by the engine",
	}, []string{"decision"})
	FilterInternalErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_filter_internal_errors_total",
		Help: "Non-fatal filter dependency failures (geo lookup, redis side-effects)",
	}, []string{"kind"})
	FilterGeoDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_filter_geo_duration_seconds",
		Help:    "Geo filter MaxMind country lookup duration (sampled 1/128 by default)",
		Buckets: []float64{0.000001, 0.000002, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001},
	})
	TTCBypassTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_ttc_bypass_total",
		Help: "Clicks accepted without impression timestamp (TTC fail-open bypass)",
	})

	// Redis Lua metrics exist so shard-level script availability and EVAL latency alerts catch filter hot-path regressions early.
	RedisLuaDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_redis_lua_duration_seconds",
		Help:    "Execution duration of Redis Lua filters",
		Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	}, []string{"shard"})
	RedisLuaNoScriptTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_redis_lua_noscript_total",
		Help: "EVALSHA NOSCRIPT fallbacks to full EVAL (script evicted or not preloaded)",
	}, []string{"shard"})
	RedisLuaScriptLoaded = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_redis_lua_script_loaded",
		Help: "1 if filter_full (unified-filter) Lua is loaded on shard via SCRIPT LOAD, else 0",
	}, []string{"shard"})
	RedisLuaFastScriptLoaded = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_redis_lua_fast_script_loaded",
		Help: "1 if budget_fast Lua is loaded on shard via SCRIPT LOAD, else 0",
	}, []string{"shard"})
	RedisLuaFastPathTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_redis_lua_fast_path_total",
		Help: "Unified filter checks routed to budget_fast.lua",
	}, []string{"shard"})
	RedisLuaFullPathTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_redis_lua_full_path_total",
		Help: "Unified filter checks routed to filter_full (unified-filter) Lua",
	}, []string{"shard"})
	RedisLuaFastDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_redis_lua_fast_duration_seconds",
		Help:    "Execution duration of budget_fast Lua filters",
		Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05},
	}, []string{"shard"})
	RedisOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_redis_ops_total",
		Help: "Unified-filter Redis EvalSha round trips per shard (includes budget-miss retries)",
	}, []string{"shard"})
	RedisCampaignOpsSampledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_redis_campaign_ops_sampled_total",
		Help: "Sampled unified-filter Redis ops by campaign for per-shard top-N dashboards (see METRICS_HISTOGRAM_SAMPLE_MASK)",
	}, []string{"shard", "campaign_id"})
	TrackerCampaignSpendSampledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_tracker_campaign_spend_micro_sampled_total",
		Help: "Sampled accepted spend in micro-units by campaign and shard for hot-campaign detection",
	}, []string{"shard", "campaign_id"})

	// Budget cache metrics exist so overspend risk is visible when warm paths miss and hot-path fallbacks hit PostgreSQL or the registry.
	BudgetCacheWarmTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_budget_cache_warm_total",
		Help: "Redis budget:campaign:* keys inserted via SET NX during registry sync warm",
	}, []string{"type"})
	BudgetCacheMissTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_budget_cache_miss_total",
		Help: "Unified filter Lua budget key misses (return -1)",
	})
	BudgetCacheMissPGTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_budget_cache_miss_pg_total",
		Help: "Budget cache misses resolved via PostgreSQL GetByID on hot path",
	})
	BudgetCacheRegistryRecoverTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_budget_cache_registry_recover_total",
		Help: "Budget cache misses recovered from in-memory registry without PostgreSQL",
	})

	// Registry sync lag exists so stale campaign config in the hot path triggers alerts before delivery rules drift from the database.
	RegistrySyncLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_registry_sync_lag_seconds",
		Help:    "Registry sync lag between database update and cache loading",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	})
	RegistryWarmDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_registry_warm_duration_seconds",
		Help:    "Duration of incremental registry warm (UpdateAndWarmCampaign)",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0},
	})

	// Geo provider status exists so production deploys that accidentally run the mock geo provider are caught before targeting goes wrong.
	GeoProviderStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_geo_provider_status",
		Help: "Status of the geo provider: 1 = real MaxMind, 0 = mock",
	})

	// Tracker health probe mirrors gnet /health DEGRADED redis= state for paging without scraping each replica.
	TrackerHealthDegraded = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_tracker_health_degraded",
		Help: "1 when tracker /health is DEGRADED (postgres or any redis shard ping failed), else 0",
	})
	TrackerRedisShardHealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_tracker_redis_shard_healthy",
		Help: "Per-shard redis ping from tracker health probe: 1 healthy, 0 unreachable",
	}, []string{"shard"})

	// Management outbox lag exposes cold-path sync delay before hot-path Redis drifts from Postgres.
	ManagementOutboxPendingTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_management_outbox_pending_total",
		Help: "Count of outbox_events rows in PENDING status awaiting Redis propagation",
	})
	ManagementOutboxOldestPendingSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_management_outbox_oldest_pending_seconds",
		Help: "Age in seconds of the oldest PENDING outbox event (0 when queue empty)",
	})
	BlacklistReplicationLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_blacklist_replication_lag_seconds",
		Help:    "Outbox-to-Redis blacklist fan-out latency (HR-BL)",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	})

	ManagementOpsAlertEnqueueFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_management_ops_alert_enqueue_failures_total",
		Help: "Failed notifier enqueue attempts from OpsAlerter and Alertmanager webhook",
	})

	AdminFanoutSourcesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_admin_fanout_sources_total",
		Help: "Fan-out source polls per admin route",
	}, []string{"route"})
	AdminFanoutPartialTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_admin_fanout_partial_total",
		Help: "Fan-out responses with at least one source failure",
	}, []string{"route"})
	AdminFanoutLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_admin_fanout_latency_seconds",
		Help:    "End-to-end fan-out request latency per admin route",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"})

	GeoIPUpdateErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_geoip_update_errors_total",
		Help: "MaxMind GeoIP database update failures",
	})

	GeoIPReloadErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_geoip_reload_errors_total",
		Help: "GeoIP hot-reload failures in the tracker watcher",
	})

	// Fraud scoring metrics exist so ops can alert on IVT mix, signal prevalence, and L1 auto-reject rate.
	FraudScoreHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_fraud_score_histogram",
		Help:    "Accumulated fraud score (0-100) per scored request",
		Buckets: []float64{0, 15, 30, 45, 60, 75, 90, 100},
	})
	FraudTierTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_fraud_tier_total",
		Help: "Fraud score tier assignments after filter scoring",
	}, []string{"tier"})
	FraudReasonTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_fraud_reason_total",
		Help: "Fraud signal contributions by stable reason code",
	}, []string{"reason"})
	L1RejectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_l1_reject_total",
		Help: "L1 auto-reject decisions (dual high-confidence or L3 blocklist)",
	})

	// Broker HA metrics exist so split-brain, replication lag, and fsync stalls surface before log-shipper backpressure grows.
	BrokerProduceTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_produce_total",
		Help: "Broker produce attempts by topic and status",
	}, []string{"topic", "status"})
	BrokerFetchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_fetch_total",
		Help: "Broker fetch requests by topic",
	}, []string{"topic"})
	BrokerActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_broker_active_connections",
		Help: "Current number of active broker TCP client connections",
	})
	BrokerFsyncDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_broker_fsync_duration_seconds",
		Help:    "Duration of partition log fsync operations",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
	})
	BrokerReplicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_replication_lag_messages",
		Help: "Leader log_hwm minus local next offset (messages behind on this node)",
	}, []string{"topic"})
	BrokerLeaderEpoch = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_leader_epoch",
		Help: "Current fencing epoch when this node is leader for the topic (0 when follower)",
	}, []string{"topic", "node_id"})
	BrokerActiveLeader = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_active_leader",
		Help: "1 when this node is elected leader for the topic, else 0",
	}, []string{"topic", "node_id"})
	BrokerLeaderReady = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_leader_ready",
		Help: "1 when elected leader has caught up to log_hwm and may accept writes",
	}, []string{"topic", "node_id"})
	BrokerDiskWritable = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_broker_disk_writable",
		Help: "1 when the broker data directory is writable, else 0",
	})
	BrokerConnectionsRejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_broker_connections_rejected_total",
		Help: "TCP connections closed because max-connections limit was reached",
	})
	BrokerReplicationErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_replication_errors_total",
		Help: "Follower replication failures by topic and reason",
	}, []string{"topic", "reason"})
	BrokerProduceDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_broker_produce_duration_seconds",
		Help:    "End-to-end produce handler latency on the broker",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
	}, []string{"topic"})
	BrokerFetchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_broker_fetch_duration_seconds",
		Help:    "End-to-end fetch handler latency on the broker",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
	}, []string{"topic"})
	BrokerLeaderElectionTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_leader_election_total",
		Help: "Leader term acquisitions per topic (SETNX wins with epoch bump)",
	}, []string{"topic"})
	BrokerReplicationCatchupSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_broker_replication_catchup_seconds",
		Help:    "Time for a new leader to become ready after failover",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
	}, []string{"topic"})
	BrokerRetentionDeletedSegments = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_broker_retention_deleted_segments_total",
		Help: "Sealed log segments removed by the retention worker",
	})
	BrokerRetentionDiskUsageBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_retention_disk_usage_bytes",
		Help: "On-disk bytes for topic partition logs after the latest retention pass",
	}, []string{"topic"})
	BrokerRetentionOldestSegmentAgeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_retention_oldest_segment_age_seconds",
		Help: "Age of the oldest segment file for the topic",
	}, []string{"topic"})
	BrokerConsumerLagMessages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_consumer_lag_messages",
		Help: "Leader high watermark minus committed offset for a consumer group",
	}, []string{"topic", "group"})
	BrokerConsumerCommitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_consumer_commits_total",
		Help: "Successful consumer offset commits",
	}, []string{"topic", "group"})
	BrokerIngestMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_ingest_messages_total",
		Help: "Broker log records parsed by processor bridge",
	}, []string{"topic", "group", "event_type"})
	BrokerIngestParseErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_ingest_parse_errors_total",
		Help: "Unrecognized broker payloads on the processor bridge",
	}, []string{"topic", "group"})
	BrokerIngestCommitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_ingest_commits_total",
		Help: "Processor bridge offset commits after successful batch handling",
	}, []string{"topic", "group"})
	BrokerShadowMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_broker_shadow_messages_total",
		Help: "Broker events counted in shadow mode without store writes",
	}, []string{"topic", "group"})
	BrokerIngestDivergenceMessages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_ingest_divergence_messages",
		Help: "Redis stream length minus broker committed offset (shadow validation)",
	}, []string{"topic", "group"})
	BrokerIngestDivergenceHigh = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_broker_ingest_divergence_high",
		Help: "1 when broker/redis ingest divergence exceeds configured threshold",
	}, []string{"topic", "group"})

	// RTB auction metrics exist so bid-path fill rate and scan cost are visible before Redis integration cutover.
	RtbAuctionDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_rtb_auction_duration_seconds",
		Help:    "In-process RTB auction latency",
		Buckets: []float64{0.000001, 0.0000025, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001},
	})
	RtbAuctionNoBidTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_rtb_auction_no_bid_total",
		Help: "RTB auctions that did not clear a winner",
	}, []string{"reason"})
	RtbAuctionWinTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_rtb_auction_win_total",
		Help: "RTB auctions that cleared a winner and spent budget",
	})
	RtbAuctionCandidatesScanned = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_rtb_auction_candidates_scanned",
		Help:    "Campaign rows examined per auction in the geo shard",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
	})
	RtbBudgetSpendRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_rtb_budget_spend_rejected_total",
		Help: "Final CAS budget debits rejected after a winner was selected",
	})
	// Shadow-mode divergence metrics validate RTB winners against client campaign_id before live cutover.
	RtbShadowWinnerMismatchTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_rtb_shadow_winner_mismatch_total",
		Help: "RTB shadow auctions where the eval winner differs from the client campaign_id",
	})
	RtbShadowNoBidTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_rtb_shadow_no_bid_total",
		Help: "RTB shadow eval returned no-bid while the client supplied a campaign_id",
	}, []string{"reason"})
	RtbShadowPriceDeltaMicro = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_rtb_shadow_price_delta_micro",
		Help:    "Absolute clearing price minus payload bid_micro on shadow wins (sampled)",
		Buckets: []float64{1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000},
	})
	RtbBudgetReconcileDivergenceMicro = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_rtb_budget_reconcile_divergence_micro",
		Help:    "Absolute Redis minus RTB campaign budget delta on reconcile samples",
		Buckets: []float64{1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000},
	})
	RtbBudgetReconcileHigh = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_rtb_budget_reconcile_high",
		Help: "1 when sampled Redis/RTB budget divergence exceeds configured threshold",
	})
	RtbBudgetReconcileSamplesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_rtb_budget_reconcile_samples_total",
		Help: "Campaign budget reconcile samples completed",
	})
	TrackerLocalQuotaBlockTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_tracker_local_quota_block_total",
		Help: "Total number of events blocked locally by tracker quota cache",
	})

	UDPControlPacketsReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_packets_received_total",
		Help: "UDP control datagrams received on tracker",
	})
	UDPControlPacketsAppliedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_packets_applied_total",
		Help: "UDP control datagrams applied to ingress snapshot",
	})
	UDPControlCorruptTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_corrupt_total",
		Help: "Malformed or invalid UDP control datagrams dropped",
	})
	UDPControlStaleDropTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_stale_drop_total",
		Help: "Out-of-order UDP epochs dropped (epoch <= current)",
	})
	UDPControlStaleTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_stale_total",
		Help: "UDP control channel marked STALE (no valid packet for 2x sync interval)",
	})
	UDPControlRecoveredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_recovered_total",
		Help: "UDP control channel recovered from STALE to OK",
	})
	UDPControlGapTightenTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_gap_tighten_total",
		Help: "Epoch gap applied immediately because limits tightened",
	})
	UDPControlLoosenBlockedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_loosen_blocked_total",
		Help: "Epoch gap loosen rejected without CONFIG_SNAPSHOT",
	})
	UDPControlSnapshotAppliedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_snapshot_applied_total",
		Help: "CONFIG_SNAPSHOT epochs applied after gap or request",
	})
	UDPControlConfigRequestTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_config_request_total",
		Help: "CONFIG_REQUEST datagrams sent by tracker",
	})
	UDPControlEpochLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_udp_control_epoch_lag",
		Help: "Management epoch minus tracker applied epoch",
	})
	UDPControlPublishTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_control_publish_total",
		Help: "QUOTA_EPOCH / CONFIG_SNAPSHOT bursts sent by management",
	})
	RegionOutboxDeliveredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_region_outbox_delivered_total",
		Help: "Outbox events applied to a regional Redis cell by RegionOutboxRelay",
	})
	RegionOutboxDeliveryLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_region_outbox_delivery_lag_seconds",
		Help:    "Outbox created_at to regional DELIVERED latency",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	})
	UDPIngressAcquireTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_ingress_acquire_total",
		Help: "Ingress quota checks passed (lock-free per worker cell)",
	})
	UDPIngressRejectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_udp_ingress_reject_total",
		Help: "Ingress quota rejections when per-shard worker cell exceeds epoch limit",
	})

	QuotaDriftDetectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_quota_drift_detected_total",
		Help: "Campaign quota PG vs Redis drift events beyond chunk_size",
	})
	QuotaRepairEnqueuedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_quota_repair_enqueued_total",
		Help: "QUOTA_REPAIR outbox events enqueued by ReconWorker",
	})
	QuotaRepairAppliedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_quota_repair_applied_total",
		Help: "QUOTA_REPAIR outbox events applied successfully",
	})
	QuotaDeadShardReleaseTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_quota_dead_shard_release_total",
		Help: "PG quota rows released after dead-shard quorum confirmed",
	})

	ProcessorStreamLagSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_processor_stream_lag_seconds",
		Help: "Current stream processing lag in seconds",
	})
	MicroBatchPaused = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_micro_batch_paused",
		Help: "Whether the micro-batch scoring is paused due to stream lag (1=paused, 0=running)",
	})
	MicroBatchProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_micro_batch_processed_total",
		Help: "Total number of events processed by the micro-batcher",
	})
	MicroBatchBoostsWrittenTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_micro_batch_boosts_written_total",
		Help: "Total number of score boosts written to Redis from the micro-batcher",
	})
	CHSpoolAppendTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_ch_spool_append_total",
		Help: "ClickHouse batches durably spooled to mmap WAL during outages",
	})
	CHSpoolReplayTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_ch_spool_replay_total",
		Help: "ClickHouse spool WAL batches replayed after recovery",
	})
	CHSpoolRotateTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_ch_spool_rotate_total",
		Help: "ClickHouse spool segment rotations during long outages",
	})
	CHSpoolSegments = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_ch_spool_segments",
		Help: "Current ClickHouse spool segment count (active + sealed)",
	})
	ProcessorStreamXLen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_processor_stream_xlen",
		Help: "Redis stream length (XLEN) per shard",
	}, []string{"shard"})
	ProcessorPgAcquireWaitSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_processor_pg_acquire_wait_seconds",
		Help:    "Wait time to acquire a processor-global Postgres write slot (alias of ad_processor_write_acquire_wait_seconds{backend=\"postgres\"})",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	ProcessorWriteAcquireWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_processor_write_acquire_wait_seconds",
		Help:    "Wait time to acquire a processor-global store write slot",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	}, []string{"backend"})
	ProcessorStreamBackpressureActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ad_processor_stream_backpressure_active",
		Help: "Stream consumer paused XREADGROUP while store circuit is open (1=active)",
	}, []string{"group"})

	EdgeBlocklistSkipAllowlistedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "edge_blocklist_skip_allowlisted_total",
		Help: "Total number of blocklist sync attempts skipped because the IP is allowlisted",
	})

	EdgeTarpitDelaySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "edge_tarpit_delay_seconds",
		Help:    "Duration of tarpit delays introduced on suspicious requests",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 12),
	})

	// Cost sync metrics track network ingest health for M16 buy/sell-side ROI pipeline.
	CostSyncRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_cost_sync_runs_total",
		Help: "Cost sync runs by outcome (success, failed)",
	}, []string{"status"})
	CostSyncRowsImported = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_cost_sync_rows_imported_total",
		Help: "Campaign cost line items imported from network APIs",
	})
	CostSyncDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_cost_sync_duration_seconds",
		Help:    "Duration of one network cost sync run",
		Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 90, 120},
	}, []string{"network"})
	CostSyncReconciliationDelta = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_cost_sync_reconciliation_delta_micro_total",
		Help: "Absolute micro-unit delta applied via RECONCILIATION_ADJUST entries",
	})
	CostSyncCHErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_cost_sync_ch_errors_total",
		Help: "ClickHouse cost_snapshots insert failures",
	})

	// LedgerBatchPauseTotal counts campaigns paused when a consolidated spend flush cannot complete.
	LedgerBatchPauseTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ledger_batch_pause_total",
		Help: "Campaigns paused after ledger batch flush partial failure or insufficient balance",
	})
	SyncLedgerBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_sync_ledger_batch_size",
		Help:    "Campaign count per consolidated ledger flush Postgres transaction",
		Buckets: []float64{1, 2, 4, 8, 16, 32},
	})
	OutboxPollIntervalMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ad_outbox_poll_interval_ms",
		Help:    "Outbox worker idle poll interval in milliseconds (coefficient backoff)",
		Buckets: []float64{20, 40, 80, 160, 250},
	})
	MgmtPgGateRejectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ad_mgmt_pg_gate_rejected_total",
		Help: "Management Postgres gate rejections when LOW tier budget is exhausted",
	}, []string{"tier"})
	MgmtPgGateAcquireWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ad_mgmt_pg_gate_acquire_wait_seconds",
		Help:    "Wait time acquiring a management Postgres gate slot",
		Buckets: prometheus.DefBuckets,
	}, []string{"tier"})

	CHDiskUsedPercent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_ch_disk_used_percent",
		Help: "ClickHouse data volume used percent from system.disks",
	})
	CHJanitorRetentionDropTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_ch_janitor_retention_drop_total",
		Help: "Partitions dropped by CHPartitionJanitor retention policy",
	})
	CHJanitorEmergencyDropTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_ch_janitor_emergency_drop_total",
		Help: "Partitions dropped by CHPartitionJanitor emergency disk policy",
	})
	CHJanitorRecompressTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_ch_janitor_recompress_total",
		Help: "Partitions recompressed (OPTIMIZE FINAL) by CHPartitionJanitor off-peak pass",
	})
	CHActivePartsMax = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ad_ch_active_parts_max",
		Help: "Max active part count across table/partition (from system.parts); alert > 100",
	})
	CHSingleRowInsertsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ad_ch_single_row_inserts_total",
		Help: "ClickHouse store attempts narrowed to a single event during poison-pill binary split",
	})
)

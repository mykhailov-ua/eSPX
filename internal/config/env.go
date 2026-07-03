// Package config loads environment-backed settings shared by every service binary at startup.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// ExpectedRedisShardCount is the fixed production topology for StaticSlotSharder client sharding.
const ExpectedRedisShardCount = 4

// Secret wraps sensitive strings so structured logs never emit credentials in plain text.
type Secret string

// LogValue redacts secret fields when config values are logged through slog.
func (s Secret) LogValue() slog.Value {
	return slog.StringValue("**********")
}

// Config is the shared environment-backed settings object every service reads at startup.
type Config struct {
	ServerPort                 string
	ProcessorPort              string
	ManagementPort             string
	MetricsPort                string
	DBDSN                      Secret
	RedisAddrs                 []string
	RedisSentinelAddrs         []string
	RedisMasterNames           []string
	RedisPassword              Secret
	RedisStreamName            string
	FraudStreamName            string
	RedisGroupName             string
	RedisConsumerID            string
	CHDSN                      Secret
	AuthServerPort             string
	AuthMetricsPort            string
	Env                        string
	TrustedProxies             []string
	TokenSymmetricKey          Secret
	MaxRequestBodySize         int64
	ClickAmount                int64
	ImpressionAmount           int64
	EventBatchSize             int
	EventFlushMs               int
	StatsFlushMs               int
	MaxWorkers                 int
	CHMaxWorkers               int
	LogRetentionDays           int
	DBTrackerMaxConns          int
	DBProcessorMaxConns        int
	DBMinConns                 int
	WriteTimeoutMs             int
	FilterTimeoutMs            int
	MetricsHistogramSampleMask int
	AuditLogSampleMask         int
	IdempotencyTTLHrs          int
	RateLimitPerMin            int
	RateLimitWindowMs          int
	DuplicateTTLSec            int
	TTCMinMs                   int
	TTCFailClosed              bool
	CHBatchSize                int
	CHFlushIntervalMs          int
	PartitionPreCreateDays     int
	RegistrySyncIntervalMs     int
	BudgetSyncIntervalMs       int
	HttpReadHeaderTimeoutMs    int
	HttpReadTimeoutMs          int
	HttpWriteTimeoutMs         int
	HttpIdleTimeoutMs          int
	DefaultTokenDurationHrs    int
	StreamMaxLen               int
	RetryInitialWaitMs         int
	RetryMaxWaitMs             int
	MaxRetries                 int
	StreamMinIdleMs            int
	Argon2Memory               int
	Argon2Iterations           int
	Argon2Parallelism          int
	RedisPoolSize              int
	RedisBreakerFailThreshold  int
	RedisBreakerHalfOpen       int
	RedisBreakerOpenTimeoutMs  int
	AdminAPIKey                Secret
	AllowedOrigins             []string
	PaymentServerPort          string
	PaymentServerHost          string
	PaymentMetricsPort         string
	PaymentWebhookPort         string
	SettlementServerPort       string
	PaymentInternalToken       Secret
	SettlementInternalToken    Secret
	StripeSecretKey            Secret
	StripeWebhookSecret        Secret
	Management                 struct {
		RetentionDays          int
		CancellationFeePercent float64
		ReconIntervalMs        int
		PacingIntervalMs       int
		RateLimitRPS           float64
		RateLimitBurst         int
	}
	CampaignUpdateChannel string

	AutoscaleHighCTRThreshold   float64
	AutoscaleMinImpressions     int64
	AutoscaleLowCTRThreshold    float64
	AutoscaleMinRemainingBudget int64
	AutoscaleShiftAmount        int64

	PacingToleranceMargin float64

	CreditScoringMinAgeDays     float64
	CreditScoringMatureAgeDays  float64
	CreditScoringMidTierPercent int64
	CreditScoringMaturePercent  int64
	CreditScoringMaxCap         int64

	Lifecycle struct {
		ShutdownTimeoutMs int
		DrainTimeoutMs    int
		WaitTimeoutMs     int
	}

	Logger struct {
		Dir                   string
		Shards                int
		FlushSizeKB           int
		RotateSizeMB          int
		RotateInterval        time.Duration
		LatencyLimit          time.Duration
		PersistQueueDepth     int
		PersistEnqueueTimeout time.Duration
	}

	Broker struct {
		URL                 string
		RedisURL            string
		Topic               string
		PartitionCount      int
		ShadowMode          bool
		MaxBytes            int
		TimeoutMs           int
		ReconcileIntervalMs int
		DivergenceThreshold uint64
	}

	RtbMode                  string
	RtbBudgetAuthority       string
	RtbClearingMode          string
	RtbSnapshotPath          string
	RtbHybridMaxRpsPerNode   int
	RtbReconcileIntervalMs   int
	RtbBudgetDivergenceMicro int64
	RtbReconcileSampleSize   int
	RtbTargetingIndex        bool

	QuotaMode               string
	QuotaChunkSize          int64
	QuotaRefillThresholdPct int

	SlotMapReloadTopic      string
	SlotMapPollIntervalMs   int
	SlotMigrationEnabled    bool
	SlotMigrationIntervalMs int
	ManagementURL           string

	Notifier struct {
		Port                    string
		WorkerIntervalMs        int
		WorkerBatchSize         int
		BreakerFailThreshold    int
		BreakerSuccessThreshold int
		BreakerOpenTimeoutMs    int
		TelegramBotToken        Secret
		TelegramChatID          string
		SlackWebhookURL         Secret
		SMSProviderURL          string
		SMSAPIToken             Secret
		SMSDefaultRecipient     string
		SMTPHost                string
		SMTPPort                string
		SMTPUsername            string
		SMTPPassword            Secret
		SMTPSender              string
	}

	Billing struct {
		Port       string
		ServerHost string
	}

	BillingInternalToken Secret
}

// trimCommaList splits comma-separated env values and drops empty tokens so trailing commas do not create phantom shards.
func trimCommaList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// BrokerEnabled reports whether the processor should run the broker ingest bridge.
func (c *Config) BrokerEnabled() bool {
	return c != nil && c.Broker.URL != ""
}

// RedisSentinelEnabled reports whether Go services dial masters via Sentinel instead of REDIS_ADDRS directly.
func (c *Config) RedisSentinelEnabled() bool {
	return len(c.RedisSentinelAddrs) > 0
}

// ResolveRedisMasterNames returns Sentinel master names aligned with REDIS_ADDRS shard count.
func (c *Config) ResolveRedisMasterNames() []string {
	if len(c.RedisMasterNames) > 0 {
		return c.RedisMasterNames
	}
	names := make([]string, len(c.RedisAddrs))
	for i := range c.RedisAddrs {
		names[i] = fmt.Sprintf("espx-shard-%d", i)
	}
	return names
}

// getEnvBool reads boolean env vars with safe parsing so mis-typed values fall back instead of crashing.
func getEnvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		switch strings.ToLower(value) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}

// getEnvInt reads integer env vars with safe parsing so missing keys use service defaults.
func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return fallback
}

// getEnvFloat reads float env vars with safe parsing for tuning knobs like CTR thresholds.
func getEnvFloat(key string, fallback float64) float64 {
	if value, ok := os.LookupEnv(key); ok {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return fallback
}

// getEnvMicro converts dollar env values to micro-units so billing code stays integer-only.
func getEnvMicro(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return int64(floatVal * 1_000_000)
		}
	}
	return fallback
}

// getEnvInt64 reads int64 env vars for limits and counters that exceed 32-bit range.
func getEnvInt64(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
	}
	return fallback
}

// Load builds a validated Config from the process environment so every binary shares one tuning surface.
func Load() (*Config, error) {
	cfg := &Config{
		ServerPort:                  os.Getenv("SERVER_PORT"),
		ProcessorPort:               os.Getenv("PROCESSOR_PORT"),
		ManagementPort:              os.Getenv("MANAGEMENT_PORT"),
		DBDSN:                       Secret(os.Getenv("DB_DSN")),
		RedisAddrs:                  trimCommaList(os.Getenv("REDIS_ADDRS")),
		RedisSentinelAddrs:          trimCommaList(os.Getenv("REDIS_SENTINEL_ADDRS")),
		RedisMasterNames:            trimCommaList(os.Getenv("REDIS_MASTER_NAMES")),
		RedisPassword:               Secret(os.Getenv("REDIS_PASSWORD")),
		RedisStreamName:             os.Getenv("REDIS_STREAM_NAME"),
		FraudStreamName:             os.Getenv("FRAUD_STREAM_NAME"),
		RedisGroupName:              os.Getenv("REDIS_GROUP_NAME"),
		RedisConsumerID:             os.Getenv("REDIS_CONSUMER_ID"),
		EventBatchSize:              getEnvInt("EVENT_BATCH_SIZE", 1000),
		EventFlushMs:                getEnvInt("EVENT_FLUSH_MS", 500),
		StatsFlushMs:                getEnvInt("STATS_FLUSH_MS", 5000),
		MaxWorkers:                  getEnvInt("MAX_WORKERS", 16),
		CHMaxWorkers:                getEnvInt("CH_MAX_WORKERS", 1),
		LogRetentionDays:            getEnvInt("LOG_RETENTION_DAYS", 7),
		DBTrackerMaxConns:           getEnvInt("DB_TRACKER_MAX_CONNS", 4),
		DBProcessorMaxConns:         getEnvInt("DB_PROCESSOR_MAX_CONNS", 16),
		DBMinConns:                  getEnvInt("DB_MIN_CONNS", 2),
		WriteTimeoutMs:              getEnvInt("WRITE_TIMEOUT_MS", 5000),
		FilterTimeoutMs:             getEnvInt("FILTER_TIMEOUT_MS", 0),
		MetricsHistogramSampleMask:  getEnvInt("METRICS_HISTOGRAM_SAMPLE_MASK", 127),
		AuditLogSampleMask:          getEnvInt("AUDIT_LOG_SAMPLE_RATE", 127),
		IdempotencyTTLHrs:           getEnvInt("IDEMPOTENCY_TTL_HRS", 24),
		RateLimitPerMin:             getEnvInt("RATE_LIMIT_PER_MIN", 100),
		RateLimitWindowMs:           getEnvInt("RATE_LIMIT_WINDOW_MS", 60000),
		MaxRequestBodySize:          getEnvInt64("MAX_REQUEST_BODY_SIZE", 1048576),
		DuplicateTTLSec:             getEnvInt("DUPLICATE_TTL_SEC", 10),
		TTCMinMs:                    getEnvInt("TTC_MIN_MS", 300),
		TTCFailClosed:               getEnvBool("TTC_FAIL_CLOSED", false),
		CHDSN:                       Secret(os.Getenv("CH_DSN")),
		CHBatchSize:                 getEnvInt("CH_BATCH_SIZE", 50000),
		CHFlushIntervalMs:           getEnvInt("CH_FLUSH_INTERVAL_MS", 10000),
		AuthServerPort:              os.Getenv("AUTH_SERVER_PORT"),
		TokenSymmetricKey:           Secret(os.Getenv("TOKEN_SYMMETRIC_KEY")),
		PartitionPreCreateDays:      getEnvInt("PARTITION_PRECREATE_DAYS", 2),
		RegistrySyncIntervalMs:      getEnvInt("REGISTRY_SYNC_INTERVAL_MS", 60000),
		BudgetSyncIntervalMs:        getEnvInt("BUDGET_SYNC_INTERVAL_MS", 5000),
		HttpReadHeaderTimeoutMs:     getEnvInt("HTTP_READ_HEADER_TIMEOUT_MS", 2000),
		HttpReadTimeoutMs:           getEnvInt("HTTP_READ_TIMEOUT_MS", 5000),
		HttpWriteTimeoutMs:          getEnvInt("HTTP_WRITE_TIMEOUT_MS", 10000),
		HttpIdleTimeoutMs:           getEnvInt("HTTP_IDLE_TIMEOUT_MS", 30000),
		DefaultTokenDurationHrs:     getEnvInt("DEFAULT_TOKEN_DURATION_HRS", 24),
		ClickAmount:                 getEnvMicro("CLICK_AMOUNT", 100_000),
		ImpressionAmount:            getEnvMicro("IMPRESSION_AMOUNT", 10_000),
		StreamMaxLen:                getEnvInt("STREAM_MAX_LEN", 100000),
		RetryInitialWaitMs:          getEnvInt("RETRY_INITIAL_WAIT_MS", 100),
		RetryMaxWaitMs:              getEnvInt("RETRY_MAX_WAIT_MS", 5000),
		MaxRetries:                  getEnvInt("MAX_RETRIES", 5),
		StreamMinIdleMs:             getEnvInt("STREAM_MIN_IDLE_MS", 300000),
		Argon2Memory:                getEnvInt("ARGON2_MEMORY", 65536),
		Argon2Iterations:            getEnvInt("ARGON2_ITERATIONS", 3),
		Argon2Parallelism:           getEnvInt("ARGON2_PARALLELISM", 4),
		RedisPoolSize:               getEnvInt("REDIS_POOL_SIZE", 0),
		RedisBreakerFailThreshold:   getEnvInt("REDIS_BREAKER_FAIL_THRESHOLD", 150),
		RedisBreakerHalfOpen:        getEnvInt("REDIS_BREAKER_HALF_OPEN", 10),
		RedisBreakerOpenTimeoutMs:   getEnvInt("REDIS_BREAKER_OPEN_TIMEOUT_MS", 5000),
		AdminAPIKey:                 Secret(os.Getenv("ADMIN_API_KEY")),
		AllowedOrigins:              strings.Split(os.Getenv("ALLOWED_ORIGINS"), ","),
		TrustedProxies:              strings.Split(os.Getenv("TRUSTED_PROXIES"), ","),
		Env:                         os.Getenv("ENV"),
		AuthMetricsPort:             os.Getenv("AUTH_METRICS_PORT"),
		CampaignUpdateChannel:       os.Getenv("CAMPAIGN_UPDATE_CHANNEL"),
		AutoscaleHighCTRThreshold:   getEnvFloat("AUTOSCALE_HIGH_CTR_THRESHOLD", 0.015),
		AutoscaleMinImpressions:     getEnvInt64("AUTOSCALE_MIN_IMPRESSIONS", 100),
		AutoscaleLowCTRThreshold:    getEnvFloat("AUTOSCALE_LOW_CTR_THRESHOLD", 0.005),
		AutoscaleMinRemainingBudget: getEnvMicro("AUTOSCALE_MIN_REMAINING_BUDGET", 20.0),
		AutoscaleShiftAmount:        getEnvMicro("AUTOSCALE_SHIFT_AMOUNT", 10.0),
		PacingToleranceMargin:       getEnvFloat("PACING_TOLERANCE_MARGIN", 0.15),
		CreditScoringMinAgeDays:     getEnvFloat("CREDIT_SCORING_MIN_AGE_DAYS", 7.0),
		CreditScoringMatureAgeDays:  getEnvFloat("CREDIT_SCORING_MATURE_AGE_DAYS", 30.0),
		CreditScoringMidTierPercent: getEnvInt64("CREDIT_SCORING_MID_TIER_PERCENT", 15),
		CreditScoringMaturePercent:  getEnvInt64("CREDIT_SCORING_MATURE_PERCENT", 30),
		CreditScoringMaxCap:         getEnvMicro("CREDIT_SCORING_MAX_CAP", 10000.0),
		PaymentServerPort:           os.Getenv("PAYMENT_SERVER_PORT"),
		PaymentServerHost:           os.Getenv("PAYMENT_SERVER_HOST"),
		PaymentMetricsPort:          os.Getenv("PAYMENT_METRICS_PORT"),
		PaymentWebhookPort:          os.Getenv("PAYMENT_WEBHOOK_PORT"),
		SettlementServerPort:        os.Getenv("SETTLEMENT_SERVER_PORT"),
		PaymentInternalToken:        Secret(os.Getenv("PAYMENT_INTERNAL_TOKEN")),
		SettlementInternalToken:     Secret(os.Getenv("SETTLEMENT_INTERNAL_TOKEN")),
		StripeSecretKey:             Secret(os.Getenv("STRIPE_SECRET_KEY")),
		StripeWebhookSecret:         Secret(os.Getenv("STRIPE_WEBHOOK_SECRET")),
	}

	cfg.Logger.Dir = os.Getenv("LOGGER_DIR")
	if cfg.Logger.Dir == "" {
		cfg.Logger.Dir = "/var/log/espx"
	}
	cfg.Logger.Shards = getEnvInt("LOGGER_SHARDS", 8)
	cfg.Logger.FlushSizeKB = getEnvInt("LOGGER_FLUSH_SIZE_KB", 256)
	cfg.Logger.RotateSizeMB = getEnvInt("LOGGER_ROTATE_SIZE_MB", 512)
	cfg.Logger.RotateInterval = time.Duration(getEnvInt("LOGGER_ROTATE_INTERVAL_MIN", 60)) * time.Minute
	cfg.Logger.LatencyLimit = time.Duration(getEnvInt("LOGGER_LATENCY_LIMIT_MS", 100)) * time.Millisecond
	cfg.Logger.PersistQueueDepth = getEnvInt("LOGGER_PERSIST_QUEUE_DEPTH", 0)
	cfg.Logger.PersistEnqueueTimeout = time.Duration(getEnvInt("LOGGER_PERSIST_ENQUEUE_TIMEOUT_MS", 25)) * time.Millisecond

	cfg.Broker.URL = os.Getenv("BROKER_URL")
	cfg.Broker.RedisURL = os.Getenv("BROKER_REDIS_URL")
	cfg.Broker.Topic = os.Getenv("BROKER_TOPIC")
	cfg.Broker.PartitionCount = getEnvInt("BROKER_PARTITION_COUNT", ExpectedRedisShardCount)
	cfg.Broker.ShadowMode = getEnvBool("BROKER_SHADOW_MODE", true)
	cfg.Broker.MaxBytes = getEnvInt("BROKER_FETCH_MAX_BYTES", 1024*1024)
	cfg.Broker.TimeoutMs = getEnvInt("BROKER_TIMEOUT_MS", 5000)
	cfg.Broker.ReconcileIntervalMs = getEnvInt("BROKER_RECONCILE_INTERVAL_MS", 30000)
	cfg.Broker.DivergenceThreshold = uint64(getEnvInt64("BROKER_DIVERGENCE_THRESHOLD", 1000))
	if cfg.Broker.Topic == "" {
		cfg.Broker.Topic = "tracker-logs"
	}

	cfg.RtbMode = os.Getenv("RTB_MODE")
	cfg.RtbBudgetAuthority = os.Getenv("RTB_BUDGET_AUTHORITY")
	cfg.RtbClearingMode = os.Getenv("RTB_CLEARING_MODE")
	cfg.RtbSnapshotPath = os.Getenv("RTB_SNAPSHOT_PATH")
	cfg.RtbHybridMaxRpsPerNode = getEnvInt("RTB_HYBRID_MAX_RPS_PER_NODE", 0)
	cfg.RtbReconcileIntervalMs = getEnvInt("RTB_RECONCILE_INTERVAL_MS", 30000)
	cfg.RtbBudgetDivergenceMicro = int64(getEnvInt("RTB_BUDGET_DIVERGENCE_THRESHOLD_MICRO", 1000))
	cfg.RtbReconcileSampleSize = getEnvInt("RTB_RECONCILE_SAMPLE_SIZE", 32)
	cfg.RtbTargetingIndex = getEnvBool("RTB_TARGETING_INDEX", false)
	if cfg.RtbBudgetAuthority == "" {
		cfg.RtbBudgetAuthority = "redis"
	}

	cfg.QuotaMode = os.Getenv("QUOTA_MODE")
	if cfg.QuotaMode == "" {
		cfg.QuotaMode = "off"
	}
	cfg.QuotaChunkSize = getEnvInt64("QUOTA_CHUNK_SIZE", 0)
	cfg.QuotaRefillThresholdPct = getEnvInt("QUOTA_REFILL_THRESHOLD_PCT", 20)

	cfg.SlotMapReloadTopic = os.Getenv("SLOT_MAP_RELOAD_TOPIC")
	if cfg.SlotMapReloadTopic == "" {
		cfg.SlotMapReloadTopic = "shards:reload"
	}
	cfg.SlotMapPollIntervalMs = getEnvInt("SLOT_MAP_POLL_INTERVAL_MS", 10000)
	cfg.SlotMigrationEnabled = getEnvBool("SLOT_MIGRATION_ENABLED", true)
	cfg.SlotMigrationIntervalMs = getEnvInt("SLOT_MIGRATION_INTERVAL_MS", 30000)
	cfg.ManagementURL = os.Getenv("MANAGEMENT_URL")
	if cfg.ManagementURL == "" && cfg.ManagementPort != "" {
		cfg.ManagementURL = "http://127.0.0.1:" + cfg.ManagementPort
	}

	cfg.Notifier.Port = os.Getenv("NOTIFIER_PORT")
	if cfg.Notifier.Port == "" {
		cfg.Notifier.Port = "8085"
	}
	cfg.Notifier.WorkerIntervalMs = getEnvInt("NOTIFIER_WORKER_INTERVAL_MS", 1000)
	cfg.Notifier.WorkerBatchSize = getEnvInt("NOTIFIER_WORKER_BATCH_SIZE", 10)
	cfg.Notifier.BreakerFailThreshold = getEnvInt("NOTIFIER_BREAKER_FAIL_THRESHOLD", 3)
	cfg.Notifier.BreakerSuccessThreshold = getEnvInt("NOTIFIER_BREAKER_SUCCESS_THRESHOLD", 2)
	cfg.Notifier.BreakerOpenTimeoutMs = getEnvInt("NOTIFIER_BREAKER_OPEN_TIMEOUT_MS", 30000)
	cfg.Notifier.TelegramBotToken = Secret(os.Getenv("TELEGRAM_BOT_TOKEN"))
	cfg.Notifier.TelegramChatID = os.Getenv("TELEGRAM_CHAT_ID")
	cfg.Notifier.SlackWebhookURL = Secret(os.Getenv("SLACK_WEBHOOK_URL"))
	cfg.Notifier.SMSProviderURL = os.Getenv("SMS_PROVIDER_URL")
	cfg.Notifier.SMSAPIToken = Secret(os.Getenv("SMS_API_TOKEN"))
	cfg.Notifier.SMSDefaultRecipient = os.Getenv("SMS_DEFAULT_RECIPIENT")
	cfg.Notifier.SMTPHost = os.Getenv("SMTP_HOST")
	cfg.Notifier.SMTPPort = os.Getenv("SMTP_PORT")
	cfg.Notifier.SMTPUsername = os.Getenv("SMTP_USERNAME")
	cfg.Notifier.SMTPPassword = Secret(os.Getenv("SMTP_PASSWORD"))
	cfg.Notifier.SMTPSender = os.Getenv("SMTP_SENDER")

	cfg.Billing.Port = os.Getenv("BILLING_SERVER_PORT")
	if cfg.Billing.Port == "" {
		cfg.Billing.Port = "51054"
	}
	cfg.Billing.ServerHost = os.Getenv("BILLING_SERVER_HOST")
	if cfg.Billing.ServerHost == "" {
		cfg.Billing.ServerHost = "127.0.0.1"
	}
	cfg.BillingInternalToken = Secret(os.Getenv("BILLING_INTERNAL_TOKEN"))

	if len(cfg.AllowedOrigins) == 1 && cfg.AllowedOrigins[0] == "" {
		cfg.AllowedOrigins = []string{"https://dashboard.example.com", "http://localhost:8188"}
	}

	cfg.Management.RetentionDays = getEnvInt("MANAGEMENT_RETENTION_DAYS", 90)
	cfg.Management.CancellationFeePercent = getEnvFloat("MANAGEMENT_CANCELLATION_FEE_PERCENT", 5.0)
	cfg.Management.ReconIntervalMs = getEnvInt("RECON_WORKER_INTERVAL_MS", 3_600_000)
	cfg.Management.PacingIntervalMs = getEnvInt("PACING_CONTROLLER_INTERVAL_MS", 300_000)
	cfg.Management.RateLimitRPS = getEnvFloat("MANAGEMENT_RATE_LIMIT_RPS", 10)
	cfg.Management.RateLimitBurst = getEnvInt("MANAGEMENT_RATE_LIMIT_BURST", 50)

	cfg.Lifecycle.ShutdownTimeoutMs = getEnvInt("SHUTDOWN_TIMEOUT_MS", 15000)
	cfg.Lifecycle.DrainTimeoutMs = getEnvInt("DRAIN_TIMEOUT_MS", 10000)
	cfg.Lifecycle.WaitTimeoutMs = getEnvInt("WAIT_TIMEOUT_MS", 5000)

	if cfg.ServerPort == "" {
		return nil, errors.New("SERVER_PORT is required")
	}
	if cfg.ProcessorPort == "" {
		cfg.ProcessorPort = "8186"
	}
	if cfg.ManagementPort == "" {
		cfg.ManagementPort = "8188"
	}
	if cfg.MetricsPort == "" {
		cfg.MetricsPort = "9090"
	}
	if cfg.DBDSN == "" {
		return nil, errors.New("DB_DSN is required")
	}
	if len(cfg.RedisAddrs) == 0 {
		return nil, errors.New("REDIS_ADDRS is required")
	}
	if cfg.Env == "production" && len(cfg.RedisAddrs) != ExpectedRedisShardCount {
		return nil, fmt.Errorf("production requires exactly %d Redis shards (REDIS_ADDRS), got %d", ExpectedRedisShardCount, len(cfg.RedisAddrs))
	}
	if cfg.RedisSentinelEnabled() {
		if len(cfg.RedisMasterNames) > 0 && len(cfg.RedisMasterNames) != len(cfg.RedisAddrs) {
			return nil, fmt.Errorf("REDIS_MASTER_NAMES count (%d) must match REDIS_ADDRS (%d)", len(cfg.RedisMasterNames), len(cfg.RedisAddrs))
		}
	}

	if cfg.RedisStreamName == "" {
		cfg.RedisStreamName = "ad:events:stream"
	}
	if cfg.FraudStreamName == "" {
		cfg.FraudStreamName = "ad:fraud:stream"
	}
	if cfg.RedisGroupName == "" {
		cfg.RedisGroupName = "ad:processor:group"
	}
	if cfg.RedisConsumerID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "unknown"
		}
		cfg.RedisConsumerID = hostname + ":" + strconv.Itoa(os.Getpid())
	}

	if cfg.AuthServerPort == "" {
		cfg.AuthServerPort = "51051"
	}
	if cfg.AuthMetricsPort == "" {
		cfg.AuthMetricsPort = "9091"
	}
	if cfg.PaymentServerPort == "" {
		// Default gRPC port keeps local compose aligned with management payment client dial target.
		cfg.PaymentServerPort = "51052"
	}
	if cfg.PaymentServerHost == "" {
		// Loopback default matches host-network compose where payment and management share the host stack.
		cfg.PaymentServerHost = "127.0.0.1"
	}
	if cfg.PaymentMetricsPort == "" {
		cfg.PaymentMetricsPort = "9092"
	}
	if cfg.PaymentWebhookPort == "" {
		// Separate HTTP port isolates Stripe webhook ingress from management admin traffic.
		cfg.PaymentWebhookPort = "8187"
	}
	if cfg.SettlementServerPort == "" {
		// Settlement gRPC lives on management; payment outbox dials this port after webhook success.
		cfg.SettlementServerPort = "51053"
	}
	if cfg.Env == "" {
		cfg.Env = "development"
	}
	if cfg.TokenSymmetricKey == "" {
		return nil, errors.New("TOKEN_SYMMETRIC_KEY is required")
	}

	if cfg.FilterTimeoutMs <= 0 {
		cfg.FilterTimeoutMs = cfg.WriteTimeoutMs
	}
	if cfg.Env == "production" && cfg.FilterTimeoutMs > 100 {
		return nil, fmt.Errorf("production FILTER_TIMEOUT_MS must be <= 100 (got %d)", cfg.FilterTimeoutMs)
	}

	return cfg, nil
}

// LifecycleShutdownTimeout returns SHUTDOWN_TIMEOUT_MS for binaries that do not load the full Config.
func LifecycleShutdownTimeout() time.Duration {
	return time.Duration(getEnvInt("SHUTDOWN_TIMEOUT_MS", 15000)) * time.Millisecond
}

// LifecycleWaitTimeout returns WAIT_TIMEOUT_MS for binaries that do not load the full Config.
func LifecycleWaitTimeout() time.Duration {
	return time.Duration(getEnvInt("WAIT_TIMEOUT_MS", 5000)) * time.Millisecond
}

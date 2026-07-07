package ads

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"espx/internal/config"
	"github.com/redis/go-redis/v9"
)

// DynamicConfig holds runtime-tunable limits pushed from the management plane.
type DynamicConfig struct {
	Version              int64  `json:"version"`
	RateLimitPerMin      int    `json:"rate_limit_per_min"`
	RateLimitWindow      int    `json:"rate_limit_window_ms"`
	ClickAmount          int64  `json:"click_amount"`
	ImpressionAmount     int64  `json:"impression_amount"`
	EmergencyBreaker     bool   `json:"emergency_breaker"`
	FraudRLSuspectPct    int    `json:"fraud_rl_suspect_pct"`
	FraudRLIVTPct        int    `json:"fraud_rl_ivt_pct"`
	FraudRLBlockPct      int    `json:"fraud_rl_block_pct"`
	FraudRLRetrySuspect  int    `json:"fraud_rl_retry_suspect_sec"`
	FraudRLRetryIVT      int    `json:"fraud_rl_retry_ivt_sec"`
	FraudRLRetryBlock    int    `json:"fraud_rl_retry_block_sec"`
	ASNCDNWhitelist      string `json:"asn_cdn_whitelist"`
	ASNMobileWhitelist   string `json:"asn_mobile_whitelist"`
	TLSHashBlocklist     string `json:"tls_hash_blocklist"`
	RtbBudgetAuthority   string `json:"rtb_budget_authority"`
}

// SettingsChangeListener runs after a new dynamic config snapshot is stored.
type SettingsChangeListener func(*DynamicConfig)

// SettingsWatcher polls Redis for config changes without restarting trackers.
type SettingsWatcher struct {
	rdbs           []redis.UniversalClient
	currentVersion int64
	snapshot       atomic.Value
	onChange       []SettingsChangeListener
}

// NewSettingsWatcher seeds dynamic config from static startup values.
func NewSettingsWatcher(rdbs []redis.UniversalClient, initial *config.Config) *SettingsWatcher {
	sw := &SettingsWatcher{
		rdbs: rdbs,
	}

	sw.snapshot.Store(&DynamicConfig{
		Version:          0,
		RateLimitPerMin:  initial.RateLimitPerMin,
		RateLimitWindow:  initial.RateLimitWindowMs,
		ClickAmount:      initial.ClickAmount,
		ImpressionAmount: initial.ImpressionAmount,
		EmergencyBreaker: false,
	})

	return sw
}

// AddChangeListener registers a callback invoked after each successful config reload.
func (sw *SettingsWatcher) AddChangeListener(fn SettingsChangeListener) {
	if fn == nil {
		return
	}
	sw.onChange = append(sw.onChange, fn)
}

// Get returns the current immutable config snapshot; callers must not mutate it.
func (sw *SettingsWatcher) Get() *DynamicConfig {
	return sw.snapshot.Load().(*DynamicConfig)
}

// Start polls Redis on interval until the context is cancelled.
func (sw *SettingsWatcher) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sw.sync(ctx)
		}
	}
}

// readConfigVersion returns config:version from the first responsive Redis shard.
func (sw *SettingsWatcher) readConfigVersion(ctx context.Context) (int64, redis.UniversalClient, error) {
	for i, rdb := range sw.rdbs {
		if rdb == nil {
			continue
		}
		v, err := rdb.Get(ctx, "config:version").Int64()
		if err == nil {
			return v, rdb, nil
		}
		if err != redis.Nil {
			slog.Warn("failed to check config version on redis shard", "shard", i, "error", err)
		}
	}
	return 0, nil, redis.Nil
}

// readConfigValues loads config:values from the given shard, falling back across the pool on error.
func (sw *SettingsWatcher) readConfigValues(ctx context.Context, preferred redis.UniversalClient) (map[string]string, error) {
	if preferred != nil {
		data, err := preferred.HGetAll(ctx, "config:values").Result()
		if err == nil {
			return data, nil
		}
	}
	for i, rdb := range sw.rdbs {
		if rdb == nil || rdb == preferred {
			continue
		}
		data, err := rdb.HGetAll(ctx, "config:values").Result()
		if err == nil {
			return data, nil
		}
		slog.Warn("failed to fetch config values on redis shard", "shard", i, "error", err)
	}
	return nil, redis.Nil
}

// sync reloads config from Redis when the version advances.
func (sw *SettingsWatcher) sync(ctx context.Context) {
	v, rdb, err := sw.readConfigVersion(ctx)
	if err != nil {
		if err != redis.Nil {
			slog.Error("failed to check config version on all redis shards", "error", err)
		}
		return
	}

	if v <= atomic.LoadInt64(&sw.currentVersion) {
		return
	}

	data, err := sw.readConfigValues(ctx, rdb)
	if err != nil {
		slog.Error("failed to fetch config values from redis", "error", err)
		return
	}

	newCfg := sw.parseConfig(v, data)
	sw.snapshot.Store(newCfg)
	atomic.StoreInt64(&sw.currentVersion, v)

	for _, fn := range sw.onChange {
		fn(newCfg)
	}

	slog.Info("dynamic settings updated", "version", v)
}

// parseConfig merges Redis hash fields into a new config snapshot.
func (sw *SettingsWatcher) parseConfig(version int64, data map[string]string) *DynamicConfig {
	current := sw.Get()
	next := *current
	next.Version = version

	updateInt(&next.RateLimitPerMin, data["rate_limit_per_min"])
	updateInt(&next.RateLimitWindow, data["rate_limit_window_ms"])
	updateMicro(&next.ClickAmount, data["click_amount"])
	updateMicro(&next.ImpressionAmount, data["impression_amount"])
	updateBool(&next.EmergencyBreaker, data["emergency_breaker"])
	updateInt(&next.FraudRLSuspectPct, data["fraud_rl_suspect_pct"])
	updateInt(&next.FraudRLIVTPct, data["fraud_rl_ivt_pct"])
	updateInt(&next.FraudRLBlockPct, data["fraud_rl_block_pct"])
	updateInt(&next.FraudRLRetrySuspect, data["fraud_rl_retry_suspect_sec"])
	updateInt(&next.FraudRLRetryIVT, data["fraud_rl_retry_ivt_sec"])
	updateInt(&next.FraudRLRetryBlock, data["fraud_rl_retry_block_sec"])
	updateString(&next.ASNCDNWhitelist, data["asn_cdn_whitelist"])
	updateString(&next.ASNMobileWhitelist, data["asn_mobile_whitelist"])
	updateString(&next.TLSHashBlocklist, data["tls_hash_blocklist"])
	updateString(&next.RtbBudgetAuthority, data[systemSettingRtbBudgetAuthority])

	return &next
}

// updateInt applies a string int override when parsing succeeds.
func updateInt(target *int, val string) {
	if val == "" {
		return
	}
	if i, err := strconv.Atoi(val); err == nil {
		*target = i
	}
}

// updateMicro applies a string float dollar amount converted to micro units.
func updateMicro(target *int64, val string) {
	if val == "" {
		return
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		*target = int64(f * 1_000_000)
	}
}

// updateBool applies a string bool override when parsing succeeds.
func updateBool(target *bool, val string) {
	if val == "" {
		return
	}
	if b, err := strconv.ParseBool(val); err == nil {
		*target = b
	}
}

// updateString applies a non-empty string override.
func updateString(target *string, val string) {
	if val != "" {
		*target = val
	}
}

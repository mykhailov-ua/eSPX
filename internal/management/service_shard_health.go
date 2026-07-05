package management

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// OutboxHealthSummary is the global outbox backlog snapshot shared across shards.
type OutboxHealthSummary struct {
	Pending              int64   `json:"pending"`
	OldestPendingSeconds float64 `json:"oldest_pending_seconds"`
	LastProcessedEventID int64   `json:"last_processed_event_id"`
}

// ShardHealthStatus reports Redis connectivity and config propagation for one shard.
type ShardHealthStatus struct {
	ShardID             int     `json:"shard_id"`
	PingOK              bool    `json:"ping_ok"`
	PingError           string  `json:"ping_error,omitempty"`
	PingLatencyMs       float64 `json:"ping_latency_ms,omitempty"`
	ConfigVersion       *int64  `json:"config_version,omitempty"`
	ConfigVersionLag    int64   `json:"config_version_lag"`
	ConfigVersionSynced bool    `json:"config_version_synced"`
}

// ShardHealthReport is the ops dashboard payload for GET /admin/ops/shards.
type ShardHealthReport struct {
	EmergencyBreaker string              `json:"emergency_breaker"`
	Outbox           OutboxHealthSummary `json:"outbox"`
	Shards           []ShardHealthStatus `json:"shards"`
}

// GetShardHealth probes each Redis shard and compares config:version against processed outbox events.
func (s *Service) GetShardHealth(ctx context.Context) (ShardHealthReport, error) {
	var report ShardHealthReport
	report.Shards = make([]ShardHealthStatus, 0, len(s.rdbs))

	settings, err := s.GetSettings(ctx)
	if err != nil {
		return report, fmt.Errorf("load system settings: %w", err)
	}
	report.EmergencyBreaker = settings["emergency_breaker"]

	outbox, err := s.outboxHealthSummary(ctx)
	if err != nil {
		return report, err
	}
	report.Outbox = outbox

	for shardID, rdb := range s.rdbs {
		status := probeShardHealth(ctx, shardID, rdb, outbox.LastProcessedEventID)
		report.Shards = append(report.Shards, status)
	}
	return report, nil
}

func (s *Service) outboxHealthSummary(ctx context.Context) (OutboxHealthSummary, error) {
	var summary OutboxHealthSummary
	if s.pool == nil {
		return summary, fmt.Errorf("postgres pool not configured")
	}
	err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'PENDING')::bigint,
			COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status = 'PENDING'))), 0)::float8,
			COALESCE((SELECT MAX(id) FROM outbox_events WHERE status = 'PROCESSED'), 0)::bigint
		FROM outbox_events`,
	).Scan(&summary.Pending, &summary.OldestPendingSeconds, &summary.LastProcessedEventID)
	if err != nil {
		return summary, fmt.Errorf("query outbox health: %w", err)
	}
	return summary, nil
}

func probeShardHealth(ctx context.Context, shardID int, rdb redis.UniversalClient, lastProcessedEventID int64) ShardHealthStatus {
	status := ShardHealthStatus{ShardID: shardID}
	if rdb == nil {
		status.PingError = "redis client not configured"
		return status
	}

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	start := time.Now()
	pingErr := rdb.Ping(pingCtx).Err()
	cancel()
	status.PingLatencyMs = float64(time.Since(start).Milliseconds())

	if pingErr != nil {
		status.PingError = pingErr.Error()
		return status
	}
	status.PingOK = true

	versionCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	version, err := rdb.Get(versionCtx, redisConfigVersionKey).Int64()
	if err == redis.Nil {
		if lastProcessedEventID > 0 {
			status.ConfigVersionLag = lastProcessedEventID
		}
		return status
	}
	if err != nil {
		status.PingOK = false
		status.PingError = fmt.Sprintf("read %s: %v", redisConfigVersionKey, err)
		return status
	}

	status.ConfigVersion = &version
	if version >= lastProcessedEventID {
		status.ConfigVersionSynced = true
		status.ConfigVersionLag = 0
	} else {
		status.ConfigVersionLag = lastProcessedEventID - version
	}
	return status
}

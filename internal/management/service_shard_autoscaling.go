package management

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"espx/internal/ads"
	"espx/internal/ads/db"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ShardMetrics represents the load and resource utilization of a single Redis shard.
type ShardMetrics struct {
	ShardID   int16
	CPUUsage  float64 // 0 to 100%
	MemoryPct float64 // 0 to 100% of maxmemory
	OpsPerSec int64
	LuaP99Ms  float64
}

// ShardMetricsProvider abstracts the collection of shard metrics for testability and chaos injection.
type ShardMetricsProvider interface {
	GetMetrics(ctx context.Context, shardID int16, rdb redis.UniversalClient) (ShardMetrics, error)
}

// RealShardMetricsProvider queries actual Redis INFO statistics.
type RealShardMetricsProvider struct{}

// GetMetrics parses used_memory, maxmemory, and instantaneous_ops_per_sec from Redis.
func (p *RealShardMetricsProvider) GetMetrics(ctx context.Context, shardID int16, rdb redis.UniversalClient) (ShardMetrics, error) {
	metrics := ShardMetrics{ShardID: shardID}

	// 1. Query INFO memory
	memInfo, err := rdb.Info(ctx, "memory").Result()
	if err == nil {
		used := parseInfoInt64(memInfo, "used_memory")
		maxmem := parseInfoInt64(memInfo, "maxmemory")
		if maxmem > 0 {
			metrics.MemoryPct = (float64(used) / float64(maxmem)) * 100.0
		} else {
			// Fallback: assume 1GB maxmemory if not explicitly set in Redis config
			metrics.MemoryPct = (float64(used) / (1024 * 1024 * 1024)) * 100.0
		}
	}

	// 2. Query INFO stats
	statsInfo, err := rdb.Info(ctx, "stats").Result()
	if err == nil {
		metrics.OpsPerSec = parseInfoInt64(statsInfo, "instantaneous_ops_per_sec")
	}

	// 3. Query INFO cpu
	cpuInfo, err := rdb.Info(ctx, "cpu").Result()
	if err == nil {
		sys := parseInfoFloat64(cpuInfo, "used_cpu_sys")
		user := parseInfoFloat64(cpuInfo, "used_cpu_user")
		metrics.CPUUsage = (sys + user) * 10.0 // Simple proxy for CPU load
		if metrics.CPUUsage > 100.0 {
			metrics.CPUUsage = 100.0
		}
	}

	return metrics, nil
}

// ShardAutoscaleConfig defines thresholds for triggering automated slot map rebalancing.
type ShardAutoscaleConfig struct {
	Enabled        bool
	CPULimit       float64 // Trigger if CPU > CPULimit (e.g. 80.0)
	MemoryPctLimit float64 // Trigger if MemoryPct > MemoryPctLimit (e.g. 85.0)
	OpsLimit       int64   // Trigger if OpsPerSec > OpsLimit (e.g. 50000)
	LuaP99Limit    float64 // Trigger if LuaP99Ms > LuaP99Limit (e.g. 15.0)
	SlotsToMigrate int16   // Number of slots to migrate per autoscale event (default 16)
}

// AutoscaleShards analyzes shard metrics and automatically triggers slot map migration to rebalance load.
// Returns the new slot map version if a migration was initiated, or 0 if no action was taken.
func (s *Service) AutoscaleShards(ctx context.Context, provider ShardMetricsProvider, cfg ShardAutoscaleConfig) (int32, error) {
	if !cfg.Enabled || len(s.rdbs) <= 1 {
		return 0, nil
	}

	if provider == nil {
		provider = &RealShardMetricsProvider{}
	}

	if cfg.SlotsToMigrate <= 0 {
		cfg.SlotsToMigrate = 16
	}

	numShards := int16(len(s.rdbs))
	shardMetrics := make([]ShardMetrics, numShards)

	for i := int16(0); i < numShards; i++ {
		m, err := provider.GetMetrics(ctx, i, s.rdbs[i])
		if err != nil {
			slog.Warn("failed to fetch metrics for shard", "shard_id", i, "error", err)
			continue
		}
		shardMetrics[i] = m
	}

	// Find the most overloaded shard (source) and the least loaded shard (target)
	var maxShard int16 = -1
	var minShard int16 = -1
	var maxLoadScore float64 = -1.0
	var minLoadScore float64 = 1e18

	for i := int16(0); i < numShards; i++ {
		m := shardMetrics[i]
		// Calculate a normalized load score: max of normalized memory, ops, CPU, or Lua latency
		memScore := m.MemoryPct / cfg.MemoryPctLimit
		opsScore := float64(m.OpsPerSec) / float64(cfg.OpsLimit)
		cpuScore := m.CPUUsage / cfg.CPULimit
		luaScore := m.LuaP99Ms / cfg.LuaP99Limit

		loadScore := memScore
		if opsScore > loadScore {
			loadScore = opsScore
		}
		if cpuScore > loadScore {
			loadScore = cpuScore
		}
		if luaScore > loadScore {
			loadScore = luaScore
		}

		// Check if this shard is overloaded
		isOverloaded := m.MemoryPct > cfg.MemoryPctLimit ||
			float64(m.OpsPerSec) > float64(cfg.OpsLimit) ||
			m.CPUUsage > cfg.CPULimit ||
			m.LuaP99Ms > cfg.LuaP99Limit

		if isOverloaded && loadScore > maxLoadScore {
			maxLoadScore = loadScore
			maxShard = i
		}

		if loadScore < minLoadScore {
			minLoadScore = loadScore
			minShard = i
		}
	}

	// If no shard is overloaded, or if we only have one shard, or if the most overloaded is also the least loaded
	if maxShard == -1 || minShard == -1 || maxShard == minShard {
		return 0, nil
	}

	slog.Info("autoscaling: detected overloaded shard, initiating slot rebalancing",
		"source_shard", maxShard,
		"target_shard", minShard,
		"source_load_score", maxLoadScore,
		"target_load_score", minLoadScore,
	)

	// Retrieve active slot map version
	mapRepo := ads.NewSlotMapRepo(s.GetPool())
	activeVer, err := mapRepo.GetActiveVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get active slot map version: %w", err)
	}

	activeRows, err := mapRepo.ListVersion(ctx, activeVer)
	if err != nil {
		return 0, fmt.Errorf("failed to list active slot map rows: %w", err)
	}

	// Select slots belonging to maxShard to migrate to minShard
	var selectedSlots []int16
	for _, row := range activeRows {
		if row.ShardID == maxShard && row.State == db.RedisSlotStateACTIVE {
			selectedSlots = append(selectedSlots, row.Slot)
			if int16(len(selectedSlots)) >= cfg.SlotsToMigrate {
				break
			}
		}
	}

	if len(selectedSlots) == 0 {
		slog.Warn("autoscaling: no active slots found on overloaded shard to migrate", "shard_id", maxShard)
		return 0, nil
	}

	// Create new slot map version (draft)
	draftVer, err := s.CreateSlotMapVersion(ctx, uuid.Nil, &activeVer, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create draft slot map version: %w", err)
	}

	// Mark selected slots as MIGRATING to minShard
	err = s.MarkSlotMapMigrating(ctx, uuid.Nil, draftVer, selectedSlots, minShard)
	if err != nil {
		return 0, fmt.Errorf("failed to mark slots migrating: %w", err)
	}

	// Register migration jobs
	err = s.EnsureSlotMigrationJobs(ctx, draftVer)
	if err != nil {
		return 0, fmt.Errorf("failed to register slot migration jobs: %w", err)
	}

	// Execute copy phase
	err = s.CopyAllMigratingSlots(ctx, draftVer)
	if err != nil {
		return 0, fmt.Errorf("failed to copy slot migration data: %w", err)
	}

	// Activate the new slot map version (cutover)
	err = s.ActivateSlotMapVersion(ctx, uuid.Nil, draftVer)
	if err != nil {
		return 0, fmt.Errorf("failed to activate new slot map version: %w", err)
	}

	// Execute drain phase (cleanup old keys)
	err = s.DrainMigratingSlots(ctx, draftVer)
	if err != nil {
		// Log warning but don't fail, as the new map is already active and correct
		slog.Warn("failed to drain migrating slots, old keys will be cleaned up asynchronously", "version", draftVer, "error", err)
	}

	slog.Info("autoscaling: successfully rebalanced slots and activated new slot map version",
		"old_version", activeVer,
		"new_version", draftVer,
		"migrated_slots_count", len(selectedSlots),
	)

	return draftVer, nil
}

// Helper functions to parse Redis INFO output.
func parseInfoInt64(info, key string) int64 {
	lines := strings.Split(info, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(line, key+":") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				val, err := strconv.ParseInt(parts[1], 10, 64)
				if err == nil {
					return val
				}
			}
		}
	}
	return 0
}

func parseInfoFloat64(info, key string) float64 {
	lines := strings.Split(info, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(line, key+":") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				val, err := strconv.ParseFloat(parts[1], 64)
				if err == nil {
					return val
				}
			}
		}
	}
	return 0.0
}

// Package xdpstats publishes aggregated XDP counters for operator dashboards.
package xdpstats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisSnapshotKey = "edge:xdp:stats_snapshot"

// Snapshot is a point-in-time aggregate of per-CPU XDP stats.
type Snapshot struct {
	UpdatedAt    time.Time         `json:"updated_at"`
	Pass         uint64            `json:"pass"`
	PassAllow    uint64            `json:"pass_allowlist"`
	Drops        map[string]uint64 `json:"drops"`
	Fingerprints uint64            `json:"fingerprints"`
}

// WriteRedis stores the snapshot for management/operator dashboards.
func WriteRedis(ctx context.Context, rdb redis.Cmdable, snap Snapshot) error {
	if rdb == nil {
		return nil
	}
	snap.UpdatedAt = snap.UpdatedAt.UTC()
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, redisSnapshotKey, raw, 10*time.Minute).Err()
}

// ReadRedis loads the latest snapshot written by edge-bpf-sync.
func ReadRedis(ctx context.Context, rdb redis.Cmdable) (Snapshot, error) {
	if rdb == nil {
		return Snapshot{}, fmt.Errorf("redis client is nil")
	}
	raw, err := rdb.Get(ctx, redisSnapshotKey).Bytes()
	if err != nil {
		return Snapshot{}, err
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

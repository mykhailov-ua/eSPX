package management

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

const (
	defaultDeadShardQuorum = 90 * time.Second
	trackerBreakerOpenPct  = 0.5
)

// ShardQuorumTracker records how long per-shard outage signals persist (M3 dead-shard release).
type ShardQuorumTracker struct {
	mu             sync.Mutex
	numShards      int
	quorum         time.Duration
	pingFailSince  []time.Time
	sentinelDown   []time.Time
	breakerOpen    []time.Time
	breakerPctFunc func(ctx context.Context, shard int) float64
}

// NewShardQuorumTracker builds a tracker for N Redis shards.
func NewShardQuorumTracker(numShards int, quorum time.Duration) *ShardQuorumTracker {
	if quorum <= 0 {
		quorum = defaultDeadShardQuorum
	}
	if numShards <= 0 {
		numShards = 1
	}
	return &ShardQuorumTracker{
		numShards:     numShards,
		quorum:        quorum,
		pingFailSince: make([]time.Time, numShards),
		sentinelDown:  make([]time.Time, numShards),
		breakerOpen:   make([]time.Time, numShards),
	}
}

// SetBreakerPctFunc overrides how tracker breaker open ratio is sampled (tests inject 1.0).
func (q *ShardQuorumTracker) SetBreakerPctFunc(fn func(ctx context.Context, shard int) float64) {
	q.mu.Lock()
	q.breakerPctFunc = fn
	q.mu.Unlock()
}

// ObserveShard samples ping health, sentinel master reachability, and tracker breaker ratio.
func (q *ShardQuorumTracker) ObserveShard(ctx context.Context, shard int, rdb redis.UniversalClient) {
	if q == nil || shard < 0 || shard >= q.numShards || rdb == nil {
		return
	}
	now := time.Now()

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	pingErr := rdb.Ping(pingCtx).Err()
	cancel()

	sentinelUp := pingErr == nil
	if pingErr == nil {
		infoCtx, infoCancel := context.WithTimeout(ctx, 2*time.Second)
		if info, err := rdb.Info(infoCtx, "replication").Result(); err != nil || info == "" {
			sentinelUp = false
		}
		infoCancel()
	}

	breakerPct := q.readBreakerPct(ctx, shard, rdb)

	q.mu.Lock()
	defer q.mu.Unlock()
	q.touch(&q.pingFailSince[shard], pingErr != nil, now)
	q.touch(&q.sentinelDown[shard], !sentinelUp, now)
	q.touch(&q.breakerOpen[shard], breakerPct >= trackerBreakerOpenPct, now)
}

func (q *ShardQuorumTracker) readBreakerPct(ctx context.Context, shard int, rdb redis.UniversalClient) float64 {
	if q.breakerPctFunc != nil {
		return q.breakerPctFunc(ctx, shard)
	}
	key := fmt.Sprintf("control:tracker_breaker_open_pct:%d", shard)
	v, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return f
}

func (q *ShardQuorumTracker) touch(slot *time.Time, active bool, now time.Time) {
	if active {
		if slot.IsZero() {
			*slot = now
		}
		return
	}
	*slot = time.Time{}
}

// DeadShardConfirmed is true when ping fail, sentinel down, and >=50% tracker breakers open for >=quorum.
func (q *ShardQuorumTracker) DeadShardConfirmed(shard int) bool {
	if q == nil || shard < 0 || shard >= q.numShards {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	return heldFor(q.pingFailSince[shard], now, q.quorum) &&
		heldFor(q.sentinelDown[shard], now, q.quorum) &&
		heldFor(q.breakerOpen[shard], now, q.quorum)
}

func heldFor(since time.Time, now time.Time, d time.Duration) bool {
	return !since.IsZero() && now.Sub(since) >= d
}

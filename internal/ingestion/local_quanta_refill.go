package ingestion

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/metrics"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

//go:embed local-quota-refill.lua
var localQuotaRefillLua string

var localQuotaRefillScript = redis.NewScript(localQuotaRefillLua)

const (
	refillSignalCap    = 4096
	refillJitterMaxMs  = 50
	refillPollInterval = 25 * time.Millisecond
	refillLockTTL      = 10 * time.Second
)

type refillSignal struct {
	campaignID uuid.UUID
	shard      int
}

// QuotaRefillWorker pulls Redis quota chunks into the local ledger (M8-02, M8-10).
type QuotaRefillWorker struct {
	ledger       *LocalQuantaLedger
	rdbs         []redis.UniversalClient
	sharder      Sharder
	baseChunk    int64
	thresholdPct int
	maxPerShard  int
	floorMicro   int64
	ceilingMicro int64
	strictEnter  int64
	strict       *LocalQuantaStrict
	flusher      *LocalQuantaFlusher

	signalCh chan refillSignal
	stopCh   chan struct{}
	wg       sync.WaitGroup

	shardInflight [16]atomic.Int32
}

// QuotaRefillConfig wires refill worker parameters.
type QuotaRefillConfig struct {
	BaseChunkMicro int64
	ThresholdPct   int
	MaxPerShard    int
	FloorMicro     int64
	CeilingMicro   int64
	StrictEnter    int64 // QUOTA_STRICT_THRESHOLD_MICRO for AdaptiveChunkSizeStrict (M14-15)
}

// NewQuotaRefillWorker starts the cold-path refill loop.
func NewQuotaRefillWorker(
	ledger *LocalQuantaLedger,
	rdbs []redis.UniversalClient,
	sharder Sharder,
	cfg QuotaRefillConfig,
) *QuotaRefillWorker {
	if ledger == nil || len(rdbs) == 0 || sharder == nil {
		return nil
	}
	chunk := cfg.BaseChunkMicro
	if chunk <= 0 {
		chunk = 5_000_000
	}
	maxShard := cfg.MaxPerShard
	if maxShard <= 0 {
		maxShard = 4
	}
	w := &QuotaRefillWorker{
		ledger:       ledger,
		rdbs:         rdbs,
		sharder:      sharder,
		baseChunk:    chunk,
		thresholdPct: cfg.ThresholdPct,
		maxPerShard:  maxShard,
		floorMicro:   cfg.FloorMicro,
		ceilingMicro: cfg.CeilingMicro,
		strictEnter:  cfg.StrictEnter,
		signalCh:     make(chan refillSignal, refillSignalCap),
		stopCh:       make(chan struct{}),
	}
	w.wg.Add(1)
	go w.loop()
	return w
}

// SetStrictMode wires hysteresis + flusher for M14-15 strict-band chunk tuning.
func (w *QuotaRefillWorker) SetStrictMode(strict *LocalQuantaStrict, flusher *LocalQuantaFlusher) {
	if w == nil {
		return
	}
	w.strict = strict
	w.flusher = flusher
}

// Signal schedules an async refill for a campaign (non-blocking hot path).
func (w *QuotaRefillWorker) Signal(campaignID uuid.UUID) {
	if w == nil {
		return
	}
	shard := w.sharder.GetShard(campaignID)
	sig := refillSignal{campaignID: campaignID, shard: shard}
	select {
	case w.signalCh <- sig:
	default:
	}
}

// Close stops the refill worker.
func (w *QuotaRefillWorker) Close() {
	if w == nil {
		return
	}
	close(w.stopCh)
	w.wg.Wait()
}

func (w *QuotaRefillWorker) loop() {
	defer w.wg.Done()
	ticker := time.NewTicker(refillPollInterval)
	defer ticker.Stop()
	pending := make(map[uuid.UUID]refillSignal, 64)
	for {
		select {
		case <-w.stopCh:
			return
		case sig := <-w.signalCh:
			pending[sig.campaignID] = sig
		case <-ticker.C:
			for id, sig := range pending {
				if !w.ledger.NeedsRefill(id, w.thresholdPct) {
					delete(pending, id)
					continue
				}
				if w.tryRefill(sig) {
					delete(pending, id)
				}
			}
		}
	}
}

func (w *QuotaRefillWorker) tryRefill(sig refillSignal) bool {
	shard := sig.shard
	if shard < 0 || shard >= len(w.rdbs) {
		metrics.LocalQuotaRefillTotal.WithLabelValues("skipped").Inc()
		return true
	}
	if shard < len(w.shardInflight) {
		if int(w.shardInflight[shard].Load()) >= w.maxPerShard {
			metrics.LocalQuotaRefillHerdTotal.Inc()
			metrics.LocalQuotaRefillTotal.WithLabelValues("herd").Inc()
			return false
		}
		w.shardInflight[shard].Add(1)
		defer w.shardInflight[shard].Add(-1)
	}

	jitter := time.Duration(rand.IntN(refillJitterMaxMs)) * time.Millisecond
	time.Sleep(jitter)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rdb := w.rdbs[shard]
	lockKey := fmt.Sprintf("budget:refill_lock:%s", sig.campaignID)
	claimed, err := rdb.SetNX(ctx, lockKey, "1", refillLockTTL).Result()
	if err != nil || !claimed {
		metrics.LocalQuotaRefillTotal.WithLabelValues("skipped").Inc()
		return false
	}
	defer func() { _ = rdb.Del(ctx, lockKey).Err() }()

	quotaKey := budgetQuotaKey(sig.campaignID)
	remaining, err := rdb.Get(ctx, quotaKey).Int64()
	if err == redis.Nil {
		remaining = 0
		err = nil
	}
	if err != nil {
		metrics.LocalQuotaRefillTotal.WithLabelValues("fail").Inc()
		return false
	}

	if w.strict != nil {
		wasStrict := w.strict.IsStrict(sig.campaignID)
		w.strict.UpdateFromRedisRemaining(sig.campaignID, remaining)
		if !wasStrict && w.strict.IsStrict(sig.campaignID) && w.flusher != nil {
			w.flusher.FlushLocalQuanta(ctx, sig.campaignID, FlushReasonStrict)
		}
	}

	ema := w.ledger.RPSEMA(sig.campaignID)
	chunk := AdaptiveChunkSizeStrict(ema, w.floorMicro, w.ceilingMicro, w.baseChunk, remaining, w.strictEnter)

	res, err := localQuotaRefillScript.Run(ctx, rdb, []string{quotaKey}, chunk).Int64()
	if err != nil {
		metrics.LocalQuotaRefillTotal.WithLabelValues("fail").Inc()
		slog.Debug("local quota refill lua failed", "campaign_id", sig.campaignID, "error", err)
		return false
	}
	if res < 0 {
		metrics.LocalQuotaRefillTotal.WithLabelValues("exhausted").Inc()
		return true
	}

	w.ledger.Credit(sig.campaignID, res, chunk)
	if w.strict != nil {
		w.strict.UpdateFromRedisRemaining(sig.campaignID, remaining-res)
	}
	metrics.LocalQuotaRefillTotal.WithLabelValues("success").Inc()
	return true
}

// RecoverFromDeltas replays broker deltas into the ledger on tracker restart (M8-09).
func (w *QuotaRefillWorker) RecoverFromDeltas(deltas map[uuid.UUID]int64) {
	if w == nil || w.ledger == nil || len(deltas) == 0 {
		return
	}
	for id, micro := range deltas {
		if micro > 0 {
			w.ledger.Credit(id, micro, w.baseChunk)
		}
	}
}

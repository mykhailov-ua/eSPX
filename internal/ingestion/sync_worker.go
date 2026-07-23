package ingestion

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/dedup"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// SyncWorker flushes Redis budget deltas to Postgres without losing inflight spend.
// Campaign spend is consolidated in-memory and flushed at ledgerFlushInterval (M12).
// When pgGate is set (processor), spend TX shares the global PG budget with StreamConsumer (SEM-P2).
type SyncWorker struct {
	rdb                  redis.Cmdable
	campaignRepo         campaignmodel.CampaignRepository
	customerRepo         campaignmodel.CustomerRepository
	interval             time.Duration
	ledgerFlushInterval  time.Duration
	pgGate               *ProcessorPgGate
	maxConcurrency       int
	lockTTLSec           int
	strictThresholdMicro int64
	dirtyScanCursor      uint64
	dedup                *dedup.Adapter
	wg                   sync.WaitGroup
	syncMu               sync.Mutex
	rollupMu             sync.Mutex
	campaignRollup       map[uuid.UUID]pendingRollup
	lastLedgerFlush      time.Time
}

// NewSyncWorker creates a periodic budget sync worker for campaigns and customers.
func NewSyncWorker(
	rdb redis.Cmdable,
	campaignRepo campaignmodel.CampaignRepository,
	customerRepo campaignmodel.CustomerRepository,
	interval time.Duration,
	ledgerFlushInterval time.Duration,
	pgGate *ProcessorPgGate,
	maxConcurrency int,
) *SyncWorker {
	if maxConcurrency <= 0 && pgGate == nil {
		maxConcurrency = maxConcurrencyDefault
	}
	return &SyncWorker{
		rdb:                  rdb,
		campaignRepo:         campaignRepo,
		customerRepo:         customerRepo,
		interval:             interval,
		ledgerFlushInterval:  ledgerFlushInterval,
		pgGate:               pgGate,
		maxConcurrency:       maxConcurrency,
		lockTTLSec:           60,
		strictThresholdMicro: 5_000_000,
		campaignRollup:       make(map[uuid.UUID]pendingRollup, 64),
	}
}

// ConfigureBudgetContention sets lock TTL and strict-band threshold for PG flush contention (M3-03).
func (w *SyncWorker) ConfigureBudgetContention(lockTTLSec int, strictThresholdMicro int64) {
	if w == nil {
		return
	}
	if lockTTLSec > 0 {
		w.lockTTLSec = lockTTLSec
	}
	if strictThresholdMicro > 0 {
		w.strictThresholdMicro = strictThresholdMicro
	}
}

// Start runs the sync loop until the context is cancelled.
func (w *SyncWorker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.SyncAll(ctx)
			}
		}
	}()
}

// Wait blocks until the background goroutine exits or the context is cancelled.
func (w *SyncWorker) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SyncAll serializes campaign and customer budget flushes to avoid double application.
func (w *SyncWorker) SyncAll(ctx context.Context) {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	w.collectCampaignRollup(ctx)
	if w.shouldFlushLedger() {
		w.flushCampaignRollup(ctx)
		w.lastLedgerFlush = time.Now()
	}
	w.syncCustomers(ctx)
}

func (w *SyncWorker) shouldFlushLedger() bool {
	if w.ledgerFlushInterval == 0 {
		return true
	}
	if w.ledgerFlushInterval < 0 {
		w.ledgerFlushInterval = defaultLedgerBatchFlush
	}
	if w.lastLedgerFlush.IsZero() {
		return true
	}
	return time.Since(w.lastLedgerFlush) >= w.ledgerFlushInterval
}

// prepareSyncScript moves pending sync counters into inflight under a short-lived lock.
const prepareSyncScript = `
if redis.call("EXISTS", KEYS[3]) == 1 then
    return {"0", ""}
end

local batch = redis.call("MGET", KEYS[4], KEYS[2], KEYS[1])
local txID = batch[1]
local inflight = batch[2]
local current = batch[3]

if not txID or txID == "" then
    txID = ARGV[2]
    redis.call("SET", KEYS[4], txID)
end

local total = (tonumber(inflight) or 0) + (tonumber(current) or 0)

if total <= 0 then
    local cur_num = tonumber(current)
    if cur_num and cur_num <= 0 then
        redis.call("DEL", KEYS[1])
    end
    redis.call("DEL", KEYS[4])
    return {"0", ""}
end

local cur_num = tonumber(current) or 0
if cur_num > 0 then
    local remaining = redis.call("INCRBY", KEYS[1], -cur_num)
    redis.call("INCRBY", KEYS[2], cur_num)
    if tonumber(remaining) <= 0 then
        redis.call("DEL", KEYS[1])
    end
elseif cur_num <= 0 and current then
    redis.call("DEL", KEYS[1])
end

redis.call("SET", KEYS[3], "1", "EX", ARGV[1])
return {tostring(total), txID}
`

// commitSyncScript finalizes a Postgres write and clears sync state when counters reach zero.
const commitSyncScript = `
local remaining = redis.call("INCRBY", KEYS[1], -tonumber(ARGV[1]))
if tonumber(remaining) <= 0 then
    redis.call("DEL", KEYS[1])
end

local batch = redis.call("MGET", KEYS[5], KEYS[1])
local sync_val = batch[1]
local inflight_val = batch[2]

local sync_num = tonumber(sync_val) or 0
local inflight_num = tonumber(inflight_val) or 0

if sync_num <= 0 and inflight_num <= 0 then
    redis.call("SREM", KEYS[2], ARGV[2])
end

redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
return remaining
`

// collectCampaignRollup moves dirty campaign sync counters into inflight and stages them for batch PG flush.
func (w *SyncWorker) collectCampaignRollup(ctx context.Context) {
	sem := make(chan struct{}, w.maxConcurrency)
	if w.pgGate != nil {
		sem = nil
	}
	var wg sync.WaitGroup

	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_campaigns", w.dirtyScanCursor, "", 100).Result()
		if err != nil {
			break
		}
		w.dirtyScanCursor = nextCursor

		for _, idStr := range keys {
			wg.Add(1)
			if sem != nil {
				sem <- struct{}{}
			}
			go func(id string) {
				defer wg.Done()
				if sem != nil {
					defer func() { <-sem }()
				}
				w.stageCampaignRollup(ctx, id)
			}(idStr)
		}

		if nextCursor == 0 {
			break
		}
	}
	wg.Wait()
}

func (w *SyncWorker) stageCampaignRollup(ctx context.Context, idStr string) {
	id, amountMicro, txID, keys, redisRemaining, ok := w.prepareBudgetEntity(ctx, "campaign", idStr)
	if !ok || amountMicro <= 0 {
		return
	}

	w.rollupMu.Lock()
	if w.campaignRollup == nil {
		w.campaignRollup = make(map[uuid.UUID]pendingRollup, 64)
	}
	w.campaignRollup[id] = pendingRollup{
		amountMicro:         amountMicro,
		txID:                txID,
		idStr:               idStr,
		syncKey:             keys.syncKey,
		inFlightKey:         keys.inFlightKey,
		lockKey:             keys.lockKey,
		txKey:               keys.txKey,
		dirtySet:            keys.dirtySet,
		redisRemainingMicro: redisRemaining,
	}
	w.rollupMu.Unlock()
}

func (w *SyncWorker) flushCampaignRollup(ctx context.Context) {
	w.rollupMu.Lock()
	if len(w.campaignRollup) == 0 {
		w.rollupMu.Unlock()
		return
	}
	batch := w.campaignRollup
	w.campaignRollup = make(map[uuid.UUID]pendingRollup, len(batch))
	w.rollupMu.Unlock()

	if batcher, ok := w.campaignRepo.(spendBatchFlusher); ok {
		w.flushCampaignRollupBatched(ctx, batch, batcher)
		return
	}

	for id, entry := range batch {
		if w.pgGate != nil {
			if err := w.pgGate.Acquire(ctx); err != nil {
				w.retainCampaignRollup(id, entry)
				continue
			}
		}

		item := SpendFlushItem{
			CampaignID:          id,
			AmountMicro:         entry.amountMicro,
			TxID:                entry.txID,
			RedisRemainingMicro: entry.redisRemainingMicro,
		}
		apply, dedupErr := w.resolveSpendDedup(ctx, &item, w.shardForCampaign(id))
		if dedupErr != nil {
			if w.pgGate != nil {
				w.pgGate.Release()
			}
			w.handleCampaignFlushError(ctx, id, entry, dedupErr)
			continue
		}
		if !apply {
			if w.pgGate != nil {
				w.pgGate.Release()
			}
			w.commitRollupRedis(ctx, entry)
			continue
		}

		err := w.campaignRepo.UpdateSpend(ctx, id, item.AmountMicro, item.TxID)
		if w.pgGate != nil {
			w.pgGate.Release()
		}
		if err != nil {
			w.handleCampaignFlushError(ctx, id, entry, err)
			continue
		}
		w.commitRollupRedis(ctx, entry)
	}
}

func (w *SyncWorker) flushCampaignRollupBatched(ctx context.Context, batch map[uuid.UUID]pendingRollup, batcher spendBatchFlusher) {
	ids := make([]uuid.UUID, 0, len(batch))
	for id := range batch {
		ids = append(ids, id)
	}

	for start := 0; start < len(ids); start += maxLedgerBatchSize {
		end := start + maxLedgerBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		chunkIDs := ids[start:end]
		items := make([]SpendFlushItem, 0, len(chunkIDs))
		entries := make([]pendingRollup, 0, len(chunkIDs))
		for _, id := range chunkIDs {
			entry := batch[id]
			item := SpendFlushItem{
				CampaignID:          id,
				AmountMicro:         entry.amountMicro,
				TxID:                entry.txID,
				RedisRemainingMicro: entry.redisRemainingMicro,
				StrictFlush:         entry.redisRemainingMicro > 0 && entry.redisRemainingMicro < w.strictThresholdMicro,
			}
			apply, dedupErr := w.resolveSpendDedup(ctx, &item, w.shardForCampaign(id))
			if dedupErr != nil {
				w.retainCampaignRollup(id, entry)
				continue
			}
			if !apply {
				w.commitRollupRedis(ctx, entry)
				continue
			}
			items = append(items, item)
			entries = append(entries, entry)
		}
		if len(items) == 0 {
			continue
		}

		if w.pgGate != nil {
			if err := w.pgGate.Acquire(ctx); err != nil {
				for i, item := range items {
					w.retainCampaignRollup(item.CampaignID, entries[i])
				}
				continue
			}
		}

		outcomes, err := batcher.UpdateSpendBatch(ctx, items)
		if w.pgGate != nil {
			w.pgGate.Release()
		}
		if err != nil {
			for i, item := range items {
				w.retainCampaignRollup(item.CampaignID, entries[i])
			}
			continue
		}

		metrics.SyncLedgerBatchSize.Observe(float64(len(items)))

		for i, item := range items {
			if outcomes[i].Err != nil {
				w.handleCampaignFlushError(ctx, item.CampaignID, entries[i], outcomes[i].Err)
				continue
			}
			w.commitRollupRedis(ctx, entries[i])
		}
	}
}

func (w *SyncWorker) retainCampaignRollup(id uuid.UUID, entry pendingRollup) {
	w.rollupMu.Lock()
	defer w.rollupMu.Unlock()
	if w.campaignRollup == nil {
		w.campaignRollup = make(map[uuid.UUID]pendingRollup, 1)
	}
	if existing, ok := w.campaignRollup[id]; ok {
		entry.amountMicro += existing.amountMicro
	}
	w.campaignRollup[id] = entry
}

func (w *SyncWorker) handleCampaignFlushError(ctx context.Context, id uuid.UUID, entry pendingRollup, err error) {
	if errors.Is(err, ErrInsufficientCustomerBalance) {
		_ = w.campaignRepo.UpdateStatus(ctx, id, campaignmodel.CampaignStatusPaused)
		metrics.LedgerBatchPauseTotal.Inc()
	}
	if errors.Is(err, ErrCampaignSpendSkipped) {
		metrics.SyncLagSeconds.Set(time.Since(w.lastLedgerFlush).Seconds())
	}
	w.retainCampaignRollup(id, entry)
}

func (w *SyncWorker) commitRollupRedis(ctx context.Context, entry pendingRollup) {
	w.rdb.Eval(ctx, commitSyncScript,
		[]string{entry.inFlightKey, entry.dirtySet, entry.lockKey, entry.txKey, entry.syncKey},
		entry.amountMicro, entry.idStr)
}

type budgetEntityKeys struct {
	syncKey     string
	inFlightKey string
	lockKey     string
	txKey       string
	dirtySet    string
}

func (w *SyncWorker) prepareBudgetEntity(ctx context.Context, prefix, idStr string) (uuid.UUID, int64, string, budgetEntityKeys, int64, bool) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return uuid.UUID{}, 0, "", budgetEntityKeys{}, 0, false
	}

	keys := budgetEntityKeys{
		syncKey:     "budget:sync:" + prefix + ":" + idStr,
		inFlightKey: "budget:inflight:" + prefix + ":" + idStr,
		lockKey:     "budget:lock:" + prefix + ":" + idStr,
		txKey:       "budget:txid:" + prefix + ":" + idStr,
		dirtySet:    "budget:dirty_" + prefix + "s",
	}

	lockTTL := w.lockTTLSec
	if lockTTL <= 0 {
		lockTTL = 60
	}

	newTxID := uuid.New().String()
	res, err := w.rdb.Eval(ctx, prepareSyncScript,
		[]string{keys.syncKey, keys.inFlightKey, keys.lockKey, keys.txKey}, lockTTL, newTxID).Result()
	if err != nil {
		return uuid.UUID{}, 0, "", budgetEntityKeys{}, 0, false
	}

	arr, ok := res.([]any)
	if !ok || len(arr) < 2 {
		return uuid.UUID{}, 0, "", budgetEntityKeys{}, 0, false
	}

	amountVal, ok1 := arr[0].(string)
	txIDVal, ok2 := arr[1].(string)
	if !ok1 || !ok2 || amountVal == "0" {
		if amountVal == "0" {
			w.rdb.SRem(ctx, keys.dirtySet, idStr)
		}
		return uuid.UUID{}, 0, "", budgetEntityKeys{}, 0, false
	}

	amountMicro, err := strconv.ParseInt(amountVal, 10, 64)
	if err != nil || amountMicro <= 0 {
		return uuid.UUID{}, 0, "", budgetEntityKeys{}, 0, false
	}

	redisRemaining := int64(0)
	if prefix == "campaign" {
		remKey := BudgetCampaignKey(id)
		if rem, err := w.rdb.Get(ctx, remKey).Int64(); err == nil {
			redisRemaining = rem
		}
	}

	ttl, err := w.rdb.TTL(ctx, keys.lockKey).Result()
	if err == nil && ttl > 0 && ttl < 10*time.Second {
		_ = w.rdb.Expire(ctx, keys.lockKey, time.Duration(lockTTL)*time.Second).Err()
		metrics.SyncLockExpiredTotal.Inc()
	}

	return id, amountMicro, txIDVal, keys, redisRemaining, true
}

// maxConcurrencyDefault is the management-path parallel sync cap when no processor PG gate is set.
const maxConcurrencyDefault = 32

// BudgetLockTTLSeconds derives sync lock TTL from flush intervals (M3-14).
func BudgetLockTTLSeconds(ledgerFlushMs, budgetSyncMs int) int {
	sec := ledgerFlushMs/1000 + budgetSyncMs/1000 + 30
	if sec < 60 {
		return 60
	}
	return sec
}

// syncEntity applies one dirty budget entity through prepare, Postgres update, and commit.
func (w *SyncWorker) syncEntity(ctx context.Context, prefix string, idStr string, updateFn func(context.Context, uuid.UUID, int64, string) error) {
	id, amountMicro, txID, keys, _, ok := w.prepareBudgetEntity(ctx, prefix, idStr)
	if !ok || amountMicro <= 0 {
		return
	}

	if w.pgGate != nil {
		if err := w.pgGate.Acquire(ctx); err != nil {
			return
		}
		defer w.pgGate.Release()
	}

	if err := updateFn(ctx, id, amountMicro, txID); err == nil {
		w.rdb.Eval(ctx, commitSyncScript,
			[]string{keys.inFlightKey, keys.dirtySet, keys.lockKey, keys.txKey, keys.syncKey},
			amountMicro, idStr)
	}
}

// syncCustomers scans dirty customer keys and syncs each in parallel.
func (w *SyncWorker) syncCustomers(ctx context.Context) {
	var cursor uint64
	sem := make(chan struct{}, w.maxConcurrency)
	if w.pgGate != nil {
		sem = nil
	}
	var wg sync.WaitGroup

	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_customers", cursor, "", 100).Result()
		if err != nil {
			break
		}

		for _, idStr := range keys {
			wg.Add(1)
			if sem != nil {
				sem <- struct{}{}
			}
			go func(id string) {
				defer wg.Done()
				if sem != nil {
					defer func() { <-sem }()
				}
				w.syncEntity(ctx, "customer", id, w.customerRepo.UpdateBalance)
			}(idStr)
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
	wg.Wait()
}

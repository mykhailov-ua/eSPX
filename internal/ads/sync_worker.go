package ads

import (
	"context"
	"strconv"
	"sync"
	"time"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// SyncWorker flushes Redis budget deltas to Postgres without losing inflight spend.
type SyncWorker struct {
	rdb          redis.Cmdable
	campaignRepo domain.CampaignRepository
	customerRepo domain.CustomerRepository
	interval     time.Duration
	wg           sync.WaitGroup
	syncMu       sync.Mutex
}

// NewSyncWorker creates a periodic budget sync worker for campaigns and customers.
func NewSyncWorker(
	rdb redis.Cmdable,
	campaignRepo domain.CampaignRepository,
	customerRepo domain.CustomerRepository,
	interval time.Duration,
) *SyncWorker {
	return &SyncWorker{
		rdb:          rdb,
		campaignRepo: campaignRepo,
		customerRepo: customerRepo,
		interval:     interval,
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
	w.syncCampaigns(ctx)
	w.syncCustomers(ctx)
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

// syncEntity applies one dirty budget entity through prepare, Postgres update, and commit.
func (w *SyncWorker) syncEntity(ctx context.Context, prefix string, idStr string, updateFn func(context.Context, uuid.UUID, int64, string) error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return
	}

	syncKey := "budget:sync:" + prefix + ":" + idStr
	inFlightKey := "budget:inflight:" + prefix + ":" + idStr
	lockKey := "budget:lock:" + prefix + ":" + idStr
	txKey := "budget:txid:" + prefix + ":" + idStr
	dirtySet := "budget:dirty_" + prefix + "s"

	newTxID := uuid.New().String()

	res, err := w.rdb.Eval(ctx, prepareSyncScript, []string{syncKey, inFlightKey, lockKey, txKey}, 60, newTxID).Result()
	if err != nil {
		return
	}

	arr, ok := res.([]any)
	if !ok || len(arr) < 2 {
		return
	}

	amountVal, ok1 := arr[0].(string)
	txIDVal, ok2 := arr[1].(string)
	if !ok1 || !ok2 {
		return
	}

	if amountVal == "0" {
		w.rdb.SRem(ctx, dirtySet, idStr)
		return
	}

	amountMicro, err := strconv.ParseInt(amountVal, 10, 64)
	if err != nil || amountMicro <= 0 {
		return
	}

	if err := updateFn(ctx, id, amountMicro, txIDVal); err == nil {
		w.rdb.Eval(ctx, commitSyncScript, []string{inFlightKey, dirtySet, lockKey, txKey, syncKey}, amountMicro, idStr)
	}
}

// maxConcurrency limits parallel entity syncs to protect Postgres during dirty bursts.
const maxConcurrency = 32

// syncCampaigns scans dirty campaign keys and syncs each in parallel up to maxConcurrency.
func (w *SyncWorker) syncCampaigns(ctx context.Context) {
	var cursor uint64
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_campaigns", cursor, "", 100).Result()
		if err != nil {
			break
		}

		for _, idStr := range keys {
			wg.Add(1)
			sem <- struct{}{}
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()
				w.syncEntity(ctx, "campaign", id, w.campaignRepo.UpdateSpend)
			}(idStr)
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
	wg.Wait()
}

// syncCustomers scans dirty customer keys and syncs each in parallel up to maxConcurrency.
func (w *SyncWorker) syncCustomers(ctx context.Context) {
	var cursor uint64
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for {
		keys, nextCursor, err := w.rdb.SScan(ctx, "budget:dirty_customers", cursor, "", 100).Result()
		if err != nil {
			break
		}

		for _, idStr := range keys {
			wg.Add(1)
			sem <- struct{}{}
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()
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

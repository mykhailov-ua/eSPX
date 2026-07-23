package ingestion

import (
	"context"
	"strconv"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// BudgetReconSnapshot is an atomic Redis view of campaign budget counters (M3-02).
type BudgetReconSnapshot struct {
	Remaining int64
	Sync      int64
	Inflight  int64
	Quota     int64
	HasLock   bool
	HasFence  bool
}

// reconSnapshotScript reads budget counters in one EVALSHA to avoid interleaved Lua debits.
const reconSnapshotScript = `
local vals = redis.call("MGET", KEYS[1], KEYS[2], KEYS[3])
local remaining = tonumber(vals[1]) or 0
local sync = tonumber(vals[2]) or 0
local inflight = tonumber(vals[3]) or 0
local quota = 0
if ARGV[1] == "1" then
    quota = tonumber(redis.call("GET", KEYS[4])) or 0
end
local has_lock = redis.call("EXISTS", KEYS[5])
local has_fence = redis.call("EXISTS", KEYS[6])
return {remaining, sync, inflight, quota, has_lock, has_fence}
`

// FetchBudgetReconSnapshot loads campaign budget keys atomically from Redis.
func FetchBudgetReconSnapshot(ctx context.Context, rdb redis.Cmdable, campaignID uuid.UUID, quotaMode bool) (BudgetReconSnapshot, error) {
	idStr := campaignID.String()
	tag := campaignHashTag(campaignID)
	keys := []string{
		budgetCampaignKey(campaignID),
		campaignSyncKey(campaignID),
		tag + "budget:inflight:campaign:" + idStr,
		tag + "budget:quota:" + idStr,
		tag + "budget:lock:campaign:" + idStr,
		MigrationFenceRedisKey(campaignID),
	}
	includeQuota := "0"
	if quotaMode {
		includeQuota = "1"
	}
	res, err := rdb.Eval(ctx, reconSnapshotScript, keys, includeQuota).Result()
	if err != nil {
		return BudgetReconSnapshot{}, err
	}
	return parseBudgetReconSnapshot(res)
}

func parseBudgetReconSnapshot(res any) (BudgetReconSnapshot, error) {
	arr, ok := res.([]any)
	if !ok || len(arr) < 6 {
		return BudgetReconSnapshot{}, redis.Nil
	}
	parse := func(v any) int64 {
		switch t := v.(type) {
		case int64:
			return t
		case string:
			n, _ := strconv.ParseInt(t, 10, 64)
			return n
		default:
			return 0
		}
	}
	return BudgetReconSnapshot{
		Remaining: parse(arr[0]),
		Sync:      parse(arr[1]),
		Inflight:  parse(arr[2]),
		Quota:     parse(arr[3]),
		HasLock:   parse(arr[4]) == 1,
		HasFence:  parse(arr[5]) == 1,
	}, nil
}

// RedisBudgetRemainingTotal sums operational Redis budget terms for reconciliation.
func (s BudgetReconSnapshot) RedisBudgetRemainingTotal(brokerPending int64) int64 {
	return s.Remaining + s.Sync + s.Inflight + s.Quota + brokerPending
}

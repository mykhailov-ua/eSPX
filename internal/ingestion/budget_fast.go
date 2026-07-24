package ingestion

import (
	"context"
	_ "embed"
	"errors"
	"sync"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/metrics"

	redis "github.com/redis/go-redis/v9"
)

//go:embed budget-fast.lua
var budgetFastLua string

var budgetFastLuaAny any

const (
	budgetFastKeyCount = 12
	budgetFastArgCount = 16
)

// budgetFastScratch holds pooled buffers for one budget-fast Lua round trip.
type budgetFastScratch struct {
	wIdem, wQuota, wFence, wFrozen bufWrapper
	precheck                       luaPrecheckScratch
	args                           []any
	wrappers                       UnifiedStringWrappers
	keyVals                        [budgetFastKeyCount]StringVal
	keyArgs                        [budgetFastKeyCount]any
}

var budgetFastScratchPool = sync.Pool{
	New: func() any {
		s := &budgetFastScratch{
			args: make([]any, budgetFastArgCount),
		}
		s.wIdem.buf = make([]byte, 0, 128)
		s.wQuota.buf = make([]byte, 0, 128)
		s.wFence.buf = make([]byte, 0, 128)
		s.wFrozen.buf = make([]byte, 0, 128)
		for i := range s.keyVals {
			s.keyArgs[i] = &s.keyVals[i]
		}
		return s
	},
}

func (f *UnifiedFilter) runBudgetFastLua(
	ctx context.Context,
	evt *campaignmodel.Event,
	campInfo *campaignmodel.Campaign,
	amount any,
	rdb redis.UniversalClient,
	shard int,
	scratch *budgetFastScratch,
) error {
	wIdem := &scratch.wIdem
	wQuota := &scratch.wQuota
	args := scratch.args
	wrappers := &scratch.wrappers
	precheck := &scratch.precheck

	budgetSourceKey := campInfo.BudgetCampaignKey
	if f.quotaEnabledAny == oneAny {
		wQuota.buf = wQuota.buf[:0]
		wQuota.buf = append(wQuota.buf, "budget:quota:"...)
		wQuota.buf = appendUUID(wQuota.buf, evt.CampaignID)
		budgetSourceKey = unsafeString(wQuota.buf)
	}

	wIdem.buf = wIdem.buf[:0]
	wIdem.buf = append(wIdem.buf, "idempotency:click:"...)
	wIdem.buf = append(wIdem.buf, evt.ClickID...)
	idempotencyKey := unsafeString(wIdem.buf)

	wFence := &scratch.wFence
	wFence.buf = wFence.buf[:0]
	wFence.buf = append(wFence.buf, MigrationFenceKeyPrefix...)
	wFence.buf = appendUUID(wFence.buf, evt.CampaignID)
	migrationFenceKey := unsafeString(wFence.buf)

	wFrozen := &scratch.wFrozen
	wFrozen.buf = wFrozen.buf[:0]
	wFrozen.buf = append(wFrozen.buf, BudgetFrozenKeyPrefix...)
	wFrozen.buf = appendUUID(wFrozen.buf, evt.CampaignID)
	budgetFrozenKey := unsafeString(wFrozen.buf)

	var now time.Time
	if campInfo.Location == nil || campInfo.Location == time.UTC {
		now = CachedTimeUTC()
	} else {
		now = CachedTimeIn(campInfo.Location)
	}

	kv := scratch.keyVals[:]
	kv[0].s = budgetSourceKey
	kv[1].s = idempotencyKey
	kv[2].s = campInfo.CampaignSyncKey
	kv[3].s = campInfo.CustomerSyncKey
	kv[7].s = migrationFenceKey
	kv[8].s = budgetFrozenKey
	kv[9].s = fraudBlacklistKey

	keyArgs := scratch.keyArgs
	keyArgs[4] = &dirtyCampaignsKeyVal
	keyArgs[5] = &dirtyCustomersKeyVal
	keyArgs[6] = &f.streamKeyVal
	keyArgs[9] = &kv[9]
	maxRPDAny := f.fillLuaPrecheckKeys(evt, campInfo, now, precheck, kv[:], keyArgs[:], 11, 10)

	wrappers.clickID.s = evt.ClickID
	wrappers.evtType.s = evt.Type
	wrappers.payload.s = unsafeString(evt.Payload)
	wrappers.ip.s = evt.IP
	wrappers.ua.s = evt.UA
	wrappers.userID.s = evt.UserID

	args[0] = amount
	args[1] = f.idempotencyTTLAny
	args[2] = campInfo.IDStrAny
	args[3] = campInfo.CustomerIDStrAny
	args[4] = f.maxStreamLenAny
	args[5] = &wrappers.clickID
	args[6] = &wrappers.evtType
	args[7] = &wrappers.payload
	args[8] = &wrappers.ip
	args[9] = &wrappers.ua
	args[10] = &wrappers.userID
	args[11] = f.skipBudgetDebitAny
	args[12] = campInfo.LuaRoutingEpoch()
	args[13] = maxRPDAny
	args[14] = luaPrecheckIngressTTLAny
	args[15] = &wrappers.placementID
	wrappers.placementID.s = evt.PlacementID

	for i := 0; i < 2; i++ {
		seq := f.luaMetricsSeq.Add(1)
		sampleLua := shouldSampleHistogram(seq, f.redisObservability.sampleMask)
		var luaStart int64
		if sampleLua || f.filterSlowNs > 0 {
			luaStart = monotonicNano()
		}
		f.redisObservability.recordLuaOp(shard, evt.CampaignID, sampleLua)
		incRedisLuaTier(f.luaFastPathCounters, shard)
		res, err := f.evalFastScript(ctx, rdb, shard, evt, keyArgs, args)
		f.noteLuaEvalDuration(shard, evt.CampaignID, "fast", luaStart, sampleLua, true)
		if err != nil {
			return err
		}
		if res == -1 {
			retry, recErr := f.recoverBudgetAfterMiss(ctx, evt, rdb, budgetSourceKey, i)
			if recErr != nil {
				return recErr
			}
			if retry {
				continue
			}
			f.RecordShadowLuaOutcome(evt.CampaignID, true)
			return ErrBudgetExhausted
		}
		if handled, handleErr := f.handleLuaResult(ctx, evt, campInfo, amount, rdb, budgetSourceKey, shard, res, sampleLua); handled {
			if res == 3 {
				f.RecordShadowLuaOutcome(evt.CampaignID, true)
			}
			return handleErr
		}
	}
	return nil
}

// handleLuaResult maps unified Lua return codes to filter errors and budget-miss retries.
func (f *UnifiedFilter) handleLuaResult(
	ctx context.Context,
	evt *campaignmodel.Event,
	campInfo *campaignmodel.Campaign,
	amount any,
	rdb redis.UniversalClient,
	budgetSourceKey string,
	shard int,
	res int64,
	sampleLua bool,
) (handled bool, err error) {
	if res == -1 {
		return false, nil
	}

	metrics.FilterLuaBranchTotal.WithLabelValues(luaBranchLabel(res)).Inc()

	switch res {
	case 1:
		return true, ErrRateLimitExceeded
	case 2:
		return true, ErrDuplicateEvent
	case 3:
		if f.quotaMode == "live" {
			f.localQuotaCache.Block(evt.CampaignID, monotonicNano())
		}
		return true, ErrBudgetExhausted
	case 4:
		return true, ErrPacingExhausted
	case 5:
		return true, ErrFreqLimitExceeded
	case 6:
		addFraudSignal(evt, FraudReasonLowTTC)
		return true, nil
	case 7:
		addFraudSignal(evt, FraudReasonMissingImpTS)
		return true, nil
	case 10:
		metrics.TTCBypassTotal.Inc()
		metrics.EventsProcessed.Inc()
		f.recordAcceptedSpendIfDebited(shard, evt.CampaignID, amount, sampleLua)
		return true, nil
	case 11:
		return true, ErrMigrationFenced
	case luaReturnDailyQuota:
		return true, ErrDailyQuotaExceeded
	case luaReturnFraudSignal:
		addFraudSignal(evt, FraudReasonL3Blocklist)
		metrics.EventsProcessed.Inc()
		f.recordAcceptedSpendIfDebited(shard, evt.CampaignID, amount, sampleLua)
		return true, nil
	case luaReturnPlacement:
		return true, ErrPlacementBlocked
	case luaReturnTierDegraded:
		metrics.FilterTierDegradedTotal.Inc()
		metrics.EventsProcessed.Inc()
		f.recordAcceptedSpendIfDebited(shard, evt.CampaignID, amount, sampleLua)
		return true, nil
	default:
		metrics.EventsProcessed.Inc()
		f.recordAcceptedSpendIfDebited(shard, evt.CampaignID, amount, sampleLua)
		return true, nil
	}
}

// evalFastScript prefers pooled EVALSHA for budget-fast.lua with NOSCRIPT fallback.
func (f *UnifiedFilter) evalFastScript(ctx context.Context, rdb redis.UniversalClient, shard int, evt *campaignmodel.Event, keyArgs [budgetFastKeyCount]any, args []any) (int64, error) {
	res, err := f.evalShaPooledN(ctx, rdb, shard, evt, f.fastScriptHashAny, keyArgs[:], args, budgetFastKeyCount)
	if err != nil && isNoScriptErr(err) {
		incRedisLuaNoScript(f.luaNoScriptCounters, shard)
		return f.evalPooledN(ctx, rdb, shard, evt, budgetFastLuaAny, keyArgs[:], args, budgetFastKeyCount)
	}
	return res, err
}

// recoverBudgetAfterMiss reloads budget from registry or Postgres after Lua -1.
func (f *UnifiedFilter) recoverBudgetAfterMiss(
	ctx context.Context,
	evt *campaignmodel.Event,
	rdb redis.UniversalClient,
	budgetSourceKey string,
	attempt int,
) (retry bool, err error) {
	metrics.BudgetCacheMissTotal.Inc()
	if attempt > 0 {
		return false, ErrBudgetExhausted
	}
	if filterDeadlineExceededEvt(evt, ctx) {
		return false, context.DeadlineExceeded
	}

	recovered, recErr := tryRecoverBudgetFromRegistry(ctx, rdb, f.registry, evt.CampaignID, budgetSourceKey)
	if recErr != nil {
		return false, recErr
	}
	if recovered {
		return true, nil
	}

	if !f.pgFallbackAllowed {
		return false, ErrBudgetExhausted
	}

	dbTimeout := f.dbLookupTimeout
	if rem, ok := filterDeadlineRemainingEvt(evt, ctx); ok {
		if rem <= 0 {
			return false, context.DeadlineExceeded
		}
		if rem < dbTimeout {
			dbTimeout = rem
		}
	}

	metrics.BudgetCacheMissPGTotal.Inc()
	if f.repo == nil {
		return false, ErrBudgetExhausted
	}
	dbCtx, cancel := context.WithTimeout(ctx, dbTimeout)
	camp, err := f.repo.GetByID(dbCtx, evt.CampaignID)
	cancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, context.DeadlineExceeded
		}
		return false, err
	}

	remaining := camp.BudgetLimit - camp.CurrentSpend
	if remaining < 0 {
		remaining = 0
	}
	if err := warmBudgetKeyNX(ctx, rdb, budgetSourceKey, remaining); err != nil {
		return false, err
	}
	return true, nil
}

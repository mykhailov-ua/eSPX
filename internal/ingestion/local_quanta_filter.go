package ingestion

import (
	"context"

	"espx/internal/campaignmodel"
	"espx/internal/metrics"

	"github.com/google/uuid"
)

// LocalQuantaDeps groups M8 components wired into UnifiedFilter.
type LocalQuantaDeps struct {
	Ledger    *LocalQuantaLedger
	Strict    *LocalQuantaStrict
	Refill    *QuotaRefillWorker
	Publisher *BudgetDeltaPublisher
}

// SetLocalQuantaDeps attaches local quanta components to the filter.
func (f *UnifiedFilter) SetLocalQuantaDeps(deps LocalQuantaDeps) {
	f.localQuantaLedger = deps.Ledger
	f.localQuantaStrict = deps.Strict
	f.localQuantaRefill = deps.Refill
	f.localQuantaPublisher = deps.Publisher
}

// SetLocalQuantaMode configures LOCAL_QUOTA_MODE off|shadow|live (M8-06).
func (f *UnifiedFilter) SetLocalQuantaMode(mode string) {
	f.localQuotaMode = mode
	if f.localQuantaLedger != nil {
		f.localQuantaLedger.SetMode(mode)
	}
}

func (f *UnifiedFilter) localQuantaActive() bool {
	return f.localQuotaMode == "shadow" || f.localQuotaMode == "live"
}

func (f *UnifiedFilter) localQuantaEligible(evt *campaignmodel.Event, campInfo *campaignmodel.Campaign) bool {
	if f.localQuantaLedger == nil || !f.localQuantaActive() {
		return false
	}
	if f.quotaEnabledAny != oneAny {
		return false
	}
	if f.localQuantaStrict != nil && f.localQuantaStrict.IsStrict(evt.CampaignID) {
		return false
	}
	if !f.fastPathEnabled.Load() || f.needsFullLuaPath(evt, campInfo) {
		return false
	}
	if evt.Type != "impression" {
		return false
	}
	return true
}

func (f *UnifiedFilter) checkLocalQuanta(
	ctx context.Context,
	evt *campaignmodel.Event,
	campInfo *campaignmodel.Campaign,
	amountMicro int64,
) (handled bool, err error) {
	if !f.localQuantaEligible(evt, campInfo) {
		return false, nil
	}

	amount := amountMicro
	if amount <= 0 {
		amount = f.impressionAmountMicro
	}

	if f.localQuotaMode == "shadow" {
		localOK := f.localQuantaLedger.TrySpendLocal(evt.CampaignID, amount)
		if localOK {
			f.publishLocalDelta(evt.CampaignID, amount)
		}
		return false, nil
	}

	if !f.localQuantaLedger.TrySpendLocal(evt.CampaignID, amount) {
		if f.localQuantaRefill != nil {
			f.localQuantaRefill.Signal(evt.CampaignID)
		}
		return false, nil
	}

	metrics.LocalQuotaSpendTotal.Inc()
	f.publishLocalDelta(evt.CampaignID, amount)

	shard, err := f.resolveDebitShard(evt.CampaignID, evt.UserID, campInfo)
	if err != nil {
		return true, err
	}
	rdb := f.rdbs[shard%len(f.rdbs)]

	prevSkip := f.skipBudgetDebitAny
	f.skipBudgetDebitAny = oneAny
	fastScratch := budgetFastScratchPool.Get().(*budgetFastScratch)
	err = f.runBudgetFastLua(ctx, evt, campInfo, f.impressionAmountMicroAny, rdb, shard, fastScratch)
	f.skipBudgetDebitAny = prevSkip
	budgetFastScratchPool.Put(fastScratch)

	if err != nil {
		return true, err
	}
	return true, nil
}

func (f *UnifiedFilter) publishLocalDelta(campaignID uuid.UUID, amountMicro int64) {
	if f.localQuantaPublisher != nil {
		f.localQuantaPublisher.Publish(campaignID, amountMicro)
	}
}

// RecordShadowLuaOutcome compares shadow local spend with Lua budget result (M8-06).
func (f *UnifiedFilter) RecordShadowLuaOutcome(campaignID uuid.UUID, luaBudgetExhausted bool) {
	if f.localQuotaMode != "shadow" || f.localQuantaLedger == nil {
		return
	}
	localHad := f.localQuantaLedger.Remaining(campaignID) >= 0 && f.localQuantaLedger.HasCredit(campaignID)
	if localHad && luaBudgetExhausted {
		metrics.LocalQuotaShadowDiffTotal.Inc()
	}
}

// UpdateStrictFromRedis refreshes strict-mode hysteresis from redis_remaining.
func (f *UnifiedFilter) UpdateStrictFromRedis(campaignID uuid.UUID, redisRemaining int64) {
	if f.localQuantaStrict != nil {
		f.localQuantaStrict.UpdateFromRedisRemaining(campaignID, redisRemaining)
	}
}

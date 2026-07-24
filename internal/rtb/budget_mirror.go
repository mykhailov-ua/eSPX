package rtb

import "sync/atomic"

// BudgetSpendMirror records authoritative RTB debits for async Redis mirror (authority=rtb).
type BudgetSpendMirror interface {
	RecordSpend(campaignID CampaignID, budgetIdx uint32, priceMicro int64)
}

var globalBudgetSpendMirror atomic.Pointer[BudgetSpendMirror]

// SetBudgetSpendMirror installs the process-wide RTB→Redis budget mirror (tracker startup).
func SetBudgetSpendMirror(m BudgetSpendMirror) {
	if m == nil {
		globalBudgetSpendMirror.Store(nil)
		return
	}
	globalBudgetSpendMirror.Store(&m)
}

func recordBudgetSpendMirror(campaignID CampaignID, budgetIdx uint32, priceMicro int64) {
	ptr := globalBudgetSpendMirror.Load()
	if ptr == nil || *ptr == nil {
		return
	}
	(*ptr).RecordSpend(campaignID, budgetIdx, priceMicro)
}

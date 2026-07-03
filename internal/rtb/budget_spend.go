package rtb

import (
	"sync/atomic"
	"time"
)

const invalidCustomerBudgetIdx uint32 = ^uint32(0)

// CheckAndSpendAll debits campaign, optional customer, and optional daily caps atomically with rollback.
func (store *BudgetStore) CheckAndSpendAll(campaignIdx, customerIdx uint32, price, dailyLimit int64) bool {
	if dailyLimit > 0 {
		store.maybeRollDaily()
		if store.loadDailyHeadroom(campaignIdx, dailyLimit) < price {
			return false
		}
	}

	if !store.checkAndSpendOn(&store.budgets, campaignIdx, price) {
		return false
	}

	if customerIdx != invalidCustomerBudgetIdx {
		if !store.checkAndSpendOn(&store.customerBudgets, customerIdx, price) {
			store.creditOn(&store.budgets, campaignIdx, price)
			return false
		}
	}

	if dailyLimit > 0 {
		if !store.checkAndAddDailySpend(campaignIdx, price, dailyLimit) {
			if customerIdx != invalidCustomerBudgetIdx {
				store.creditOn(&store.customerBudgets, customerIdx, price)
			}
			store.creditOn(&store.budgets, campaignIdx, price)
			return false
		}
	}

	return true
}

func (store *BudgetStore) loadDailyHeadroom(campaignIdx uint32, dailyLimit int64) int64 {
	if dailyLimit <= 0 {
		return dailyLimit
	}
	spent := store.loadOn(&store.dailySpent, campaignIdx)
	return dailyLimit - spent
}

// LoadCustomerBudget returns the remaining shared customer pool for a slot index.
func (store *BudgetStore) LoadCustomerBudget(customerIdx uint32) int64 {
	if customerIdx == invalidCustomerBudgetIdx {
		return 0
	}
	return store.loadOn(&store.customerBudgets, customerIdx)
}

func (store *BudgetStore) checkAndSpendOn(holder *atomic.Pointer[budgetSlice], idx uint32, price int64) bool {
	slice := holder.Load()
	if idx >= uint32(len(slice.data)) {
		return false
	}
	ptr := &slice.data[idx].Value
	for {
		curr := atomic.LoadInt64(ptr)
		if curr < price {
			return false
		}
		if atomic.CompareAndSwapInt64(ptr, curr, curr-price) {
			return true
		}
	}
}

func (store *BudgetStore) creditOn(holder *atomic.Pointer[budgetSlice], idx uint32, price int64) {
	slice := holder.Load()
	if idx >= uint32(len(slice.data)) {
		return
	}
	ptr := &slice.data[idx].Value
	for {
		curr := atomic.LoadInt64(ptr)
		if atomic.CompareAndSwapInt64(ptr, curr, curr+price) {
			return
		}
	}
}

func (store *BudgetStore) checkAndAddDailySpend(idx uint32, price, dailyLimit int64) bool {
	slice := store.dailySpent.Load()
	if idx >= uint32(len(slice.data)) {
		return false
	}
	ptr := &slice.data[idx].Value
	for {
		curr := atomic.LoadInt64(ptr)
		if curr+price > dailyLimit {
			return false
		}
		if atomic.CompareAndSwapInt64(ptr, curr, curr+price) {
			return true
		}
	}
}

func (store *BudgetStore) addDailySpendLocked(idx uint32, spent int64) {
	slice := store.dailySpent.Load()
	if idx >= uint32(len(slice.data)) {
		return
	}
	atomic.StoreInt64(&slice.data[idx].Value, spent)
}

func (store *BudgetStore) loadOn(holder *atomic.Pointer[budgetSlice], idx uint32) int64 {
	slice := holder.Load()
	if idx >= uint32(len(slice.data)) {
		return 0
	}
	return atomic.LoadInt64(&slice.data[idx].Value)
}

func (store *BudgetStore) maybeRollDaily() {
	day := currentDayEpochUTC()
	if store.dailyEpoch.Load() == day {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.dailyEpoch.Load() == day {
		return
	}
	curr := store.dailySpent.Load()
	if len(curr.data) > 0 {
		cleared := make([]AlignedBudget, len(curr.data))
		store.dailySpent.Store(&budgetSlice{data: cleared})
	}
	store.dailyEpoch.Store(day)
}

func currentDayEpochUTC() uint32 {
	now := time.Now().UTC()
	return uint32(now.Year()*10000 + int(now.Month())*100 + now.Day())
}

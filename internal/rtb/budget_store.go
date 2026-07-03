package rtb

import (
	"sync"
	"sync/atomic"
)

const defaultBudgetCap = 10000

// AlignedBudget pads each slot to its own cache line so concurrent atomic updates do not false-share.
type AlignedBudget struct {
	Value int64
	_     [7]int64
}

// budgetSlice wraps the budget array so atomic.Pointer swaps can publish new backing storage safely.
type budgetSlice struct {
	data []AlignedBudget
}

// BudgetStore keeps campaign budgets in a flat slice so auction readers avoid map lookups and pointer chasing.
type BudgetStore struct {
	mu              sync.Mutex
	slots           map[CampaignID]uint32
	customerSlots   map[CustomerID]uint32
	budgets         atomic.Pointer[budgetSlice]
	customerBudgets atomic.Pointer[budgetSlice]
	dailySpent      atomic.Pointer[budgetSlice]
	dailyEpoch      atomic.Uint32
}

// NewBudgetStore prepares the shared budget backing store used by every geo shard during bidding.
func NewBudgetStore() *BudgetStore {
	store := &BudgetStore{
		slots:         make(map[CampaignID]uint32),
		customerSlots: make(map[CustomerID]uint32),
	}
	empty := &budgetSlice{data: make([]AlignedBudget, 0, defaultBudgetCap)}
	store.budgets.Store(empty)
	store.customerBudgets.Store(&budgetSlice{data: make([]AlignedBudget, 0, defaultBudgetCap)})
	store.dailySpent.Store(&budgetSlice{data: make([]AlignedBudget, 0, defaultBudgetCap)})
	return store
}

// GetOrAllocateSlot assigns a stable numeric index for a campaign so hot-path budget checks stay array-indexed.
func (store *BudgetStore) GetOrAllocateSlot(id CampaignID, initialBudget int64) uint32 {
	store.mu.Lock()
	if idx, exists := store.slots[id]; exists {
		store.mu.Unlock()
		return idx
	}
	idx := store.appendSlotLocked(normalizeBudget(initialBudget))
	store.slots[id] = idx
	store.mu.Unlock()
	return idx
}

// GetOrAllocateCustomerSlot assigns a stable index for a customer-level budget pool.
func (store *BudgetStore) GetOrAllocateCustomerSlot(id CustomerID, initialBudget int64) uint32 {
	if id == 0 {
		return invalidCustomerBudgetIdx
	}
	store.mu.Lock()
	if idx, exists := store.customerSlots[id]; exists {
		store.mu.Unlock()
		return idx
	}
	idx := store.appendCustomerSlotLocked(normalizeBudget(initialBudget))
	store.customerSlots[id] = idx
	store.mu.Unlock()
	return idx
}

// LoadBudget returns the remaining budget for a slot index.
func (store *BudgetStore) LoadBudget(idx uint32) int64 {
	return store.loadOn(&store.budgets, idx)
}

// budgetSlotExists reports whether idx refers to an allocated campaign budget slot.
func (store *BudgetStore) budgetSlotExists(idx uint32) bool {
	slice := store.budgets.Load()
	return idx < uint32(len(slice.data))
}

// CheckAndSpend reserves the clearing price only after a winner is chosen, so parallel auctions
// cannot overspend the same budget slot.
func (store *BudgetStore) CheckAndSpend(idx uint32, limit int64) bool {
	return store.checkAndSpendOn(&store.budgets, idx, limit)
}

// GetBudget exposes remaining budget for admin and sync paths that key campaigns by ID.
func (store *BudgetStore) GetBudget(id CampaignID) int64 {
	store.mu.Lock()
	idx, exists := store.slots[id]
	if !exists {
		store.mu.Unlock()
		return 0
	}
	val := store.loadOn(&store.budgets, idx)
	store.mu.Unlock()
	return val
}

// CampaignSlot returns the budget slot index for a campaign when allocated.
func (store *BudgetStore) CampaignSlot(id CampaignID) (uint32, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	idx, ok := store.slots[id]
	return idx, ok
}

// SetDailySpend publishes today's spent micros for a campaign slot from management/Redis sync.
func (store *BudgetStore) SetDailySpend(campaignIdx uint32, spent int64) {
	if spent < 0 {
		spent = 0
	}
	store.maybeRollDaily()
	store.addDailySpendLocked(campaignIdx, spent)
}

// SetBudget updates or inserts a campaign budget from management without rebuilding auction shards.
func (store *BudgetStore) SetBudget(id CampaignID, val int64) {
	store.mu.Lock()
	defer store.mu.Unlock()

	idx, exists := store.slots[id]
	if !exists {
		idx = store.appendSlotLocked(normalizeBudget(val))
		store.slots[id] = idx
		return
	}
	slice := store.budgets.Load()
	if int(idx) >= len(slice.data) {
		return
	}
	atomic.StoreInt64(&slice.data[idx].Value, normalizeBudget(val))
}

// CustomerSlot returns the shared customer pool slot index when allocated.
func (store *BudgetStore) CustomerSlot(id CustomerID) (uint32, bool) {
	if id == 0 {
		return invalidCustomerBudgetIdx, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	idx, ok := store.customerSlots[id]
	return idx, ok
}

// SetCustomerBudget updates the shared customer pool from management.
func (store *BudgetStore) SetCustomerBudget(id CustomerID, val int64) {
	if id == 0 {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	idx, exists := store.customerSlots[id]
	if !exists {
		idx = store.appendCustomerSlotLocked(normalizeBudget(val))
		store.customerSlots[id] = idx
		return
	}
	slice := store.customerBudgets.Load()
	if int(idx) >= len(slice.data) {
		return
	}
	atomic.StoreInt64(&slice.data[idx].Value, normalizeBudget(val))
}

// appendSlotLocked grows the campaign and daily slices by one slot. Caller must hold store.mu.
func (store *BudgetStore) appendSlotLocked(val int64) uint32 {
	currSlice := store.budgets.Load()
	idx := uint32(len(currSlice.data))

	newCap := cap(currSlice.data)
	if len(currSlice.data)+1 > newCap {
		if newCap == 0 {
			newCap = defaultBudgetCap
		} else {
			newCap *= 2
		}
	}

	newData := make([]AlignedBudget, len(currSlice.data)+1, newCap)
	copy(newData, currSlice.data)
	newData[idx] = AlignedBudget{Value: val}

	store.budgets.Store(&budgetSlice{data: newData})
	store.growDailyLocked(len(newData))
	return idx
}

func (store *BudgetStore) appendCustomerSlotLocked(val int64) uint32 {
	currSlice := store.customerBudgets.Load()
	idx := uint32(len(currSlice.data))

	newCap := cap(currSlice.data)
	if len(currSlice.data)+1 > newCap {
		if newCap == 0 {
			newCap = defaultBudgetCap
		} else {
			newCap *= 2
		}
	}

	newData := make([]AlignedBudget, len(currSlice.data)+1, newCap)
	copy(newData, currSlice.data)
	newData[idx] = AlignedBudget{Value: val}
	store.customerBudgets.Store(&budgetSlice{data: newData})
	return idx
}

func (store *BudgetStore) growDailyLocked(n int) {
	curr := store.dailySpent.Load()
	if len(curr.data) >= n {
		return
	}
	newData := make([]AlignedBudget, n, n*2)
	copy(newData, curr.data)
	store.dailySpent.Store(&budgetSlice{data: newData})
}

// normalizeBudget clamps negative management writes to zero.
func normalizeBudget(val int64) int64 {
	if val < 0 {
		return 0
	}
	return val
}

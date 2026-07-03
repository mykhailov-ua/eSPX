package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Guards CheckAndSpendAll rolls back campaign and customer debits when daily cap fails.
func TestCheckAndSpendAll_rollsBackCustomerOnDailyFail(t *testing.T) {
	store := NewBudgetStore()
	campaignIdx := store.GetOrAllocateSlot(1, 1000)
	customerIdx := store.GetOrAllocateCustomerSlot(9, 1000)

	ok := store.CheckAndSpendAll(campaignIdx, customerIdx, 900, 500)
	assert.False(t, ok)
	assert.Equal(t, int64(1000), store.LoadBudget(campaignIdx))
	assert.Equal(t, int64(1000), store.LoadCustomerBudget(customerIdx))
}

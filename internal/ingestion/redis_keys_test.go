package ingestion

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestCampaignKeys_CrossSlotColocation(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	keys := []string{
		budgetCampaignKey(id),
		campaignSyncKey(id),
		customerSyncKey(id, uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")),
		fcapKeyPrefix(id, ""),
		dailySpendKeyPrefix(id) + "2026-07-20",
		PlacementBlacklistKey(id),
	}
	slot := RedisClusterSlot(keys[0])
	for i, key := range keys[1:] {
		assert.Equal(t, slot, RedisClusterSlot(key), "key %d must share campaign hash slot", i+1)
	}
}

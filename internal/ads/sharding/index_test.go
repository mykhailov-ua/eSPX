package sharding

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCampaignSlotIndex_matchesGetShardSlot(t *testing.T) {
	sharder := NewStaticSlotSharder(4)
	for i := 0; i < 256; i++ {
		id := uuid.New()
		slot := CampaignSlotIndex(id)
		require.GreaterOrEqual(t, slot, int16(0))
		require.LessOrEqual(t, slot, int16(SlotMask))
		_ = sharder.GetShard(id)
	}
}

func TestFilterCampaignIDsBySlot(t *testing.T) {
	var ids []uuid.UUID
	var want []uuid.UUID
	target := int16(42)
	for len(want) < 3 {
		id := uuid.New()
		ids = append(ids, id)
		if CampaignSlotIndex(id) == target {
			want = append(want, id)
		}
	}
	got := FilterCampaignIDsBySlot(ids, target)
	require.Equal(t, want, got)
}

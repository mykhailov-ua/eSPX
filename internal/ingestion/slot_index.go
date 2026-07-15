package ingestion

import "github.com/google/uuid"

// CampaignSlotIndex returns the Fixed Slot Map index for a campaign: crc32(id) & 1023.
func CampaignSlotIndex(id uuid.UUID) int16 {
	return int16(crc32Castagnoli(&id) & SlotMask)
}

// FilterCampaignIDsBySlot returns campaign IDs whose slot index matches slot.
func FilterCampaignIDsBySlot(ids []uuid.UUID, slot int16) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(ids)/4)
	for _, id := range ids {
		if CampaignSlotIndex(id) == slot {
			out = append(out, id)
		}
	}
	return out
}

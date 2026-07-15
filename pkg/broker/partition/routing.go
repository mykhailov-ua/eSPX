// Package partition maps campaign IDs to broker partition indices.
package partition

import "hash/crc32"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const slotMask = 1023

// Slot returns the crc32 slot for a 16-byte campaign ID (matches ingestion.StaticSlotSharder).
func Slot(campaignID []byte) uint32 {
	if len(campaignID) < 16 {
		return 0
	}
	return crc32.Checksum(campaignID[:16], castagnoli) & slotMask
}

// Index maps a campaign ID to a partition index for a fixed partition count.
func Index(campaignID []byte, numPartitions int) uint16 {
	if numPartitions <= 1 {
		return 0
	}
	return uint16(int(Slot(campaignID)) % numPartitions)
}

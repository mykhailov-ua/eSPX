package ingestion

import (
	"encoding/json"
	"fmt"
	"time"
)

// DefaultSlotMapReloadTopic is the broker control topic for map cutover signals (Phase 2.2).
const DefaultSlotMapReloadTopic = "shards:reload"

// SlotMapReloadMessage is published by management after active_version changes.
type SlotMapReloadMessage struct {
	Version int32 `json:"version"`
	AtUnix  int64 `json:"at_unix"`
}

// EncodeSlotMapReloadMessage serializes a reload signal for broker produce.
func EncodeSlotMapReloadMessage(version int32) ([]byte, error) {
	msg := SlotMapReloadMessage{
		Version: version,
		AtUnix:  time.Now().Unix(),
	}
	return json.Marshal(msg)
}

// DecodeSlotMapReloadMessage parses a broker payload into a reload signal.
func DecodeSlotMapReloadMessage(payload []byte) (SlotMapReloadMessage, error) {
	var msg SlotMapReloadMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return msg, fmt.Errorf("slot map reload decode: %w", err)
	}
	if msg.Version <= 0 {
		return msg, fmt.Errorf("slot map reload decode: invalid version %d", msg.Version)
	}
	return msg, nil
}

// OpsSlotMapResponse is the compact JSON export for tracker poll and nginx edge sync.
type OpsSlotMapResponse struct {
	Version       int32    `json:"version"`
	ActiveVersion int32    `json:"active_version"`
	Slots         []uint16 `json:"slots"`
}

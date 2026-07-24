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
	Version      int32 `json:"version"`
	RoutingEpoch int64 `json:"routing_epoch,omitempty"`
	AtUnix       int64 `json:"at_unix"`
}

// EncodeSlotMapReloadMessage serializes a reload signal for broker produce.
func EncodeSlotMapReloadMessage(version int32, routingEpoch int64) ([]byte, error) {
	msg := SlotMapReloadMessage{
		Version:      version,
		RoutingEpoch: routingEpoch,
		AtUnix:       time.Now().Unix(),
	}
	return json.Marshal(msg)
}

// EncodeSlotMapReloadMessageVersion is the legacy helper without routing epoch.
func EncodeSlotMapReloadMessageVersion(version int32) ([]byte, error) {
	return EncodeSlotMapReloadMessage(version, 0)
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
	RoutingEpoch  int64    `json:"routing_epoch"`
	Slots         []uint16 `json:"slots"`
}

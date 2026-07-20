package ingestion

import (
	"sync"
	"unsafe"

	"espx/internal/ingestion/pb"
	"espx/pkg/money"
)

// MicroUnitFactor converts dollar floats to micro-dollar integers.
const MicroUnitFactor = money.MicroUnit

// SliceToMap builds O(1) country lookup sets from string slices.
func SliceToMap(slice []string) map[string]struct{} {
	if slice == nil {
		return nil
	}
	m := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		m[s] = struct{}{}
	}
	return m
}

// UnsafeString views bytes as a string without copy when the backing slice outlives use.
func UnsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// UnsafeBytes views a string as bytes without copy when the string is not mutated.
func UnsafeBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// ByteSliceValue adapts a byte slice for Redis binary marshaling without allocation.
type ByteSliceValue struct {
	b []byte
}

// MarshalBinary returns the wrapped bytes for Redis stream values.
func (v *ByteSliceValue) MarshalBinary() ([]byte, error) {
	return v.b, nil
}

// byteSliceValuePool recycles ByteSliceValue wrappers for stream XADD calls.
var byteSliceValuePool = sync.Pool{
	New: func() any {
		return new(ByteSliceValue)
	},
}

// DeepResetAdStreamEvent clears slice fields in place before returning protobuf objects to a pool.
func DeepResetAdStreamEvent(m *pb.AdStreamEvent) {
	if m == nil {
		return
	}
	m.ClickId = m.ClickId[:0]
	m.CampaignId = m.CampaignId[:0]
	m.EventType = m.EventType[:0]
	m.Payload = m.Payload[:0]
	m.Ip = m.Ip[:0]
	m.Ua = m.Ua[:0]
	m.FraudReason = m.FraudReason[:0]
	m.CreatedAtUnix = 0
	m.FraudScore = 0
	m.GhostEvent = false
}

// ClearAdStreamEvent nils large byte fields so pooled protobuf objects do not pin payload memory.
func ClearAdStreamEvent(m *pb.AdStreamEvent) {
	if m == nil {
		return
	}
	m.ClickId = nil
	m.CampaignId = nil
	m.EventType = nil
	m.Payload = nil
	m.Ip = nil
	m.Ua = nil
	m.FraudReason = nil
	m.CreatedAtUnix = 0
	m.FraudScore = 0
	m.GhostEvent = false
}

// DeepResetAdDLQEvent clears nested stream events before returning DLQ protobuf objects to a pool.
func DeepResetAdDLQEvent(m *pb.AdDLQEvent) {
	if m == nil {
		return
	}
	if m.OriginalEvent != nil {
		DeepResetAdStreamEvent(m.OriginalEvent)
	}
	m.Error = m.Error[:0]
	m.OriginalId = m.OriginalId[:0]
	m.WorkerId = m.WorkerId[:0]
	m.FailedAtUnix = 0
	m.RetryCount = 0
}

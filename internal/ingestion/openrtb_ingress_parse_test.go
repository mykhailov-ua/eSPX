package ingestion

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseOpenRTB3Payload_ReorderedNested(t *testing.T) {
	// Keys reordered vs canonical sample; nested device before item.
	payload := []byte(`{
  "category_mask": 8,
  "openrtb": {
    "ver": "3.0",
    "request": {
      "context": {
        "device": {"type": 4, "ip": "203.0.113.42"}
      },
      "id": "req-reordered",
      "item": [
        {"flr": 2.25, "id": "item-first", "spec": {"placement": {"tagid": "plc-1"}}}
      ]
    }
  },
  "deal_id": "deal-premium-1"
}`)

	minBid, deviceType, categoryMask, isOpenRTB := ParseOpenRTB3Payload(payload)
	assert.True(t, isOpenRTB)
	assert.Equal(t, int64(2250000), minBid)
	assert.Equal(t, uint8(2), deviceType)
	assert.Equal(t, uint64(8), categoryMask)

	var dealBuf [64]byte
	n := ParseDealIDBytes(payload, dealBuf[:])
	assert.Equal(t, "deal-premium-1", string(dealBuf[:n]))

	parsed := parseOpenRTB3FSM(payload)
	assert.True(t, parsed.OK)
	assert.Equal(t, "item-first", string(ortbSlice(payload, parsed.ItemIDOff, parsed.ItemIDLen)))
	assert.Equal(t, "req-reordered", string(ortbSlice(payload, parsed.RequestIDOff, parsed.RequestIDLen)))
	assert.Equal(t, "plc-1", string(ortbSlice(payload, parsed.TagIDOff, parsed.TagIDLen)))
}

func TestParseOpenRTB3Payload_DealIDZeroAlloc(t *testing.T) {
	payload := []byte(`{"openrtb":{"request":{"item":[{"id":"x","flr":1}]}},"deal_id":"deal-z"}`)
	var buf [64]byte
	allocs := testing.AllocsPerRun(1000, func() {
		_ = ParseDealIDBytes(payload, buf[:])
		_, _, _, _ = ParseOpenRTB3Payload(payload)
	})
	assert.Equal(t, float64(0), allocs)
}

func TestParseOpenRTB3Ingress(t *testing.T) {
	camp := "550e8400-e29b-41d4-a716-446655440000"
	payload := []byte(`{
  "openrtb": {
    "request": {
      "id": "req-abc",
      "item": [{"id": "` + camp + `", "flr": 1.5}],
      "context": {"device": {"type": 2}}
    }
  },
  "category_mask": 4,
  "deal_id": "deal-1"
}`)
	var req TrackRequest
	require.NoError(t, ParseOpenRTB3Ingress(&req, payload))
	assert.Equal(t, uuid.MustParse(camp), req.CampaignID)
	assert.Equal(t, "req-abc", req.ClickID)
	assert.Equal(t, "impression", req.Type)
	assert.Equal(t, payload, req.Payload)

	allocs := testing.AllocsPerRun(500, func() {
		req.Reset()
		_ = ParseOpenRTB3Ingress(&req, payload)
	})
	// Payload is aliased to input; CampaignID/ClickID from stack buffers via unsafeString.
	assert.Equal(t, float64(0), allocs)
}

func TestParseOpenRTB3Ingress_RejectsESPXNative(t *testing.T) {
	payload := []byte(`{"campaign_id":"550e8400-e29b-41d4-a716-446655440000","type":"click"}`)
	var req TrackRequest
	require.Error(t, ParseOpenRTB3Ingress(&req, payload))
}

func BenchmarkParseOpenRTB3FSM(b *testing.B) {
	payload := []byte(`{
  "openrtb": {
    "ver": "3.0",
    "request": {
      "id": "req-123456789",
      "item": [{"id": "550e8400-e29b-41d4-a716-446655440000", "flr": 1.50}],
      "context": {"device": {"type": 4}}
    }
  },
  "category_mask": 8,
  "deal_id": "deal-premium-1"
}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = parseOpenRTB3FSM(payload)
	}
}

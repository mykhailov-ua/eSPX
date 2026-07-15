package ingestion

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDecimalMicro(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"1.50", 1500000},
		{"0.5", 500000},
		{"150", 150000000},
		{"0.000005", 5},
		{"2.1234567", 2123456}, // truncated at 6 digits
		{"  0.75", 750000},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := parseDecimalMicro([]byte(tc.input))
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestParseOpenRTB3Payload(t *testing.T) {
	payload := []byte(`{
  "openrtb": {
    "ver": "3.0",
    "domainspec": "adcom",
    "domainver": "1.0",
    "request": {
      "id": "req-123456789",
      "item": [
        {
          "id": "item-1",
          "flr": 1.50,
          "spec": {
            "placement": {
              "tagid": "plc-mobile-banner"
            }
          }
        }
      ],
      "context": {
        "device": {
          "type": 4,
          "ip": "203.0.113.42"
        }
      }
    }
  },
  "category_mask": 8
}`)

	minBid, deviceType, categoryMask, isOpenRTB := ParseOpenRTB3Payload(payload)
	assert.True(t, isOpenRTB)
	assert.Equal(t, int64(1500000), minBid)
	assert.Equal(t, uint8(2), deviceType) // mapped from 4 (Phone) to 2 (Mobile)
	assert.Equal(t, uint64(8), categoryMask)
}

func TestParseOpenRTB3Payload_NotOpenRTB(t *testing.T) {
	payload := []byte(`{"category_mask":4,"bid_micro":100}`)
	_, _, _, isOpenRTB := ParseOpenRTB3Payload(payload)
	assert.False(t, isOpenRTB)
}

func TestParseOpenRTB3Payload_ZeroAlloc(t *testing.T) {
	payload := []byte(`{
  "openrtb": {
    "ver": "3.0",
    "domainspec": "adcom",
    "domainver": "1.0",
    "request": {
      "id": "req-123456789",
      "item": [
        {
          "id": "item-1",
          "flr": 1.50,
          "spec": {
            "placement": {
              "tagid": "plc-mobile-banner"
            }
          }
        }
      ],
      "context": {
        "device": {
          "type": 4,
          "ip": "203.0.113.42"
        }
      }
    }
  },
  "category_mask": 8
}`)

	allocs := testing.AllocsPerRun(1000, func() {
		_, _, _, _ = ParseOpenRTB3Payload(payload)
	})
	assert.Equal(t, float64(0), allocs)
}

func TestParseDealID(t *testing.T) {
	payload := []byte(`{"deal_id":"deal-premium-1","bid_micro":100}`)
	assert.Equal(t, "deal-premium-1", ParseDealID(payload))
	assert.Equal(t, "", ParseDealID(nil))
}

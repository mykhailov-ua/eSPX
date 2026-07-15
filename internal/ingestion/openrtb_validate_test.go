package ingestion

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validBidRequest26() []byte {
	return []byte(`{
  "id": "req-26-001",
  "imp": [{
    "id": "1",
    "banner": {"w": 300, "h": 250}
  }],
  "site": {"id": "site-1", "page": "https://example.com"},
  "device": {"ua": "Mozilla/5.0"},
  "cur": ["USD"]
}`)
}

func validBidRequest30() []byte {
	return []byte(`{
  "openrtb": {
    "ver": "3.0",
    "domainspec": "adcom",
    "domainver": "1.0",
    "request": {
      "id": "req-30-001",
      "cur": ["USD"],
      "item": [{
        "id": "item-1",
        "flr": 1.50,
        "spec": {"placement": {"tagid": "tag-1"}}
      }],
      "context": {
        "device": {"type": 4, "ip": "203.0.113.42"}
      }
    }
  },
  "category_mask": 8
}`)
}

func TestValidateOpenRTB26_valid(t *testing.T) {
	res := ValidateOpenRTBBidRequest(validBidRequest26())
	assert.True(t, res.Valid)
	assert.Equal(t, "2.6", res.Version)
	assert.Empty(t, res.Errors)
}

func TestValidateOpenRTB26_missingID(t *testing.T) {
	payload := []byte(`{"imp":[{"id":"1","banner":{"w":300,"h":250}}]}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.False(t, res.Valid)
	assert.Contains(t, res.Errors[0], "BidRequest.id is required")
}

func TestValidateOpenRTB26_missingImp(t *testing.T) {
	payload := []byte(`{"id":"x","imp":[]}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.False(t, res.Valid)
	assert.Contains(t, res.Errors[0], "imp must contain at least one")
}

func TestValidateOpenRTB26_impMissingFormat(t *testing.T) {
	payload := []byte(`{"id":"x","imp":[{"id":"1"}]}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.False(t, res.Valid)
	assert.Contains(t, strings.Join(res.Errors, " "), "banner, video, audio, or native")
}

func TestValidateOpenRTB26_multipleInventory(t *testing.T) {
	payload := []byte(`{
	  "id":"x",
	  "imp":[{"id":"1","banner":{"w":1,"h":1}}],
	  "site":{"id":"s"},
	  "app":{"id":"a"}
	}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.False(t, res.Valid)
	assert.Contains(t, res.Errors[0], "at most one of site, app, or dooh")
}

func TestValidateOpenRTB26_invalidJSON(t *testing.T) {
	res := ValidateOpenRTBBidRequest([]byte(`{not json`))
	assert.False(t, res.Valid)
	assert.Equal(t, "invalid JSON", res.Errors[0])
}

func TestValidateOpenRTB26_errorCap(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"id":"","imp":[`)
	for i := 0; i < 60; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"id":""}`)
	}
	b.WriteString(`]}`)
	res := ValidateOpenRTBBidRequest([]byte(b.String()))
	assert.False(t, res.Valid)
	assert.LessOrEqual(t, len(res.Errors), maxOpenRTBValidationErrors)
}

func TestValidateOpenRTB30_valid(t *testing.T) {
	res := ValidateOpenRTBBidRequest(validBidRequest30())
	assert.True(t, res.Valid)
	assert.Equal(t, "3.0", res.Version)
}

func TestValidateOpenRTB30_rejectsNonUSDEUR(t *testing.T) {
	payload := []byte(`{
	  "openrtb": {
	    "request": {
	      "id": "r1",
	      "cur": ["GBP"],
	      "item": [{"id": "1", "flr": 1.0}]
	    }
	  }
	}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.False(t, res.Valid)
	assert.Contains(t, strings.Join(res.Errors, " "), "GBP")
	assert.Contains(t, strings.Join(res.Errors, " "), "USD and EUR")
}

func TestValidateOpenRTB30_allowsEUR(t *testing.T) {
	payload := []byte(`{
	  "openrtb": {
	    "request": {
	      "id": "r1",
	      "cur": ["EUR"],
	      "item": [{"id": "1"}]
	    }
	  }
	}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.True(t, res.Valid)
}

func TestValidateOpenRTB30_defaultUSDOmitted(t *testing.T) {
	payload := []byte(`{
	  "openrtb": {
	    "request": {
	      "id": "r1",
	      "item": [{"id": "1"}]
	    }
	  }
	}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.True(t, res.Valid)
}

func TestValidateOpenRTB30_missingItem(t *testing.T) {
	payload := []byte(`{"openrtb":{"request":{"id":"r1","item":[]}}}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.False(t, res.Valid)
	require.NotEmpty(t, res.Errors)
	assert.Contains(t, res.Errors[0], "item must contain at least one")
}

func TestValidateOpenRTB30_parseCrossCheck(t *testing.T) {
	payload := []byte(`{
	  "openrtb": {
	    "request": {
	      "id": "r1",
	      "item": [{"id": "1", "flr": "not-a-number"}]
	    }
	  }
	}`)
	res := ValidateOpenRTBBidRequest(payload)
	assert.False(t, res.Valid)
}

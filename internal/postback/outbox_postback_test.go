package postback

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostbackPayload_UnmarshalJSON_legacyPayout(t *testing.T) {
	var p PostbackPayload
	err := json.Unmarshal([]byte(`{"payout":10.5,"click_id":"c1"}`), &p)
	require.NoError(t, err)
	assert.Equal(t, int64(10_500_000), p.PayoutMicro)
}

func TestPostbackPayload_UnmarshalJSON_payoutMicro(t *testing.T) {
	var p PostbackPayload
	err := json.Unmarshal([]byte(`{"payout_micro":2500000,"click_id":"c1"}`), &p)
	require.NoError(t, err)
	assert.Equal(t, int64(2_500_000), p.PayoutMicro)
}

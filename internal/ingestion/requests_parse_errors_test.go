package ingestion

import (
	"net/http"
	"testing"

	"espx/internal/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseTrackRequestJSON_ErrorMatrix guards malformed JSON bodies (M6-07).
func TestParseTrackRequestJSON_ErrorMatrix(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{name: "truncated", payload: []byte(`{"campaign_id":"`)},
		{name: "wrong_type_campaign_id", payload: []byte(`{"campaign_id":123,"type":"click"}`)},
		{name: "null_campaign", payload: []byte(`{"campaign_id":null,"type":"click"}`)},
		{name: "invalid_uuid", payload: []byte(`{"campaign_id":"not-a-uuid","type":"click"}`)},
		{name: "array_instead_object", payload: []byte(`[]`)},
		{name: "boolean_type", payload: []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":true}`)},
		{name: "unclosed_object", payload: []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001"`)},
		{name: "unclosed_string", payload: []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click`)},
		{name: "number_root", payload: []byte(`42`)},
		{name: "bad_payload_object", payload: []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click","payload":{`)},
		{name: "escaped_key_rejected", payload: []byte(`{"campaign\u005fid":"00000000-0000-0000-0000-000000000001","type":"click"}`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var req TrackRequest
			errStd := ParseTrackRequestJSON(&req, tc.payload)
			var reqOpt TrackRequest
			errOpt := ParseTrackRequestJSONOpt(&reqOpt, tc.payload)
			require.Error(t, errStd, "expected standard parse error")
			require.Error(t, errOpt, "expected opt parse error")
			assert.Equal(t, errStd.Error(), errOpt.Error(), "Opt must match standard on errors")
		})
	}
}

func TestParseTrackRequestJSON_ErrorMatrix_Handler400(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewStaticSlotSharder(1), "fraud", nil)
	status, _ := PostTrackGnetJSON(h, []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":true}`))
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestParseTrackRequestJSON_ValidUnicodeLiteral(t *testing.T) {
	payload := []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click","user_id":"\u0041"}`)
	var req TrackRequest
	require.NoError(t, ParseTrackRequestJSON(&req, payload))
	assert.Equal(t, `\u0041`, req.UserID)
}

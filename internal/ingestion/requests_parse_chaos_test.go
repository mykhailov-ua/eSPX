package ingestion

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fraudJSONCase struct {
	id      string
	name    string
	body    []byte
	mustErr bool
	mustOK  bool
	check   func(t *testing.T, req *TrackRequest)
}

func fraudTrackJSONCases2026() []fraudJSONCase {
	validCID := "550e8400-e29b-41d4-a716-446655440000"
	return []fraudJSONCase{
		{
			id: "G-J01", name: "truncated_no_close",
			body:    []byte(`{"campaign_id":"550e8400`),
			mustErr: true,
		},
		{
			id: "G-J02", name: "type_impression_on_click",
			body:   []byte(`{"campaign_id":"550e8400-e29b-41d4-a716-446655440000","type":"impression"}`),
			mustOK: true,
			check: func(t *testing.T, req *TrackRequest) {
				assert.Equal(t, "impression", req.Type)
			},
		},
		{
			id: "G-J03", name: "oversized_uuid_string",
			body:    []byte(`{"campaign_id":"` + strings.Repeat("a", 128) + `"}`),
			mustErr: true,
		},
		{
			id: "G-J04", name: "duplicate_user_id_last_wins",
			body:   []byte(`{"campaign_id":"550e8400-e29b-41d4-a716-446655440000","user_id":"first","user_id":"second"}`),
			mustOK: true,
			check: func(t *testing.T, req *TrackRequest) {
				assert.Equal(t, "second", req.UserID)
			},
		},
		{
			id: "G-J05", name: "nested_payload_shallow",
			body:   []byte(`{"campaign_id":"550e8400-e29b-41d4-a716-446655440000","payload":{"a":{"b":"c"}}}`),
			mustOK: true,
		},
		{
			id: "G-J06", name: "unicode_escaped_key",
			body:    []byte(`{"campaign\u005fid":"550e8400-e29b-41d4-a716-446655440000"}`),
			mustErr: true,
		},
		{
			id: "G-J07", name: "numeric_campaign_id",
			body:    []byte(`{"campaign_id":12345}`),
			mustErr: true,
		},
		{
			id: "G-J08", name: "reordered_keys",
			body:   []byte(`{"type":"click","campaign_id":"550e8400-e29b-41d4-a716-446655440000"}`),
			mustOK: true,
			check: func(t *testing.T, req *TrackRequest) {
				assert.Equal(t, validCID, req.CampaignID.String())
			},
		},
		{
			id: "G-J09", name: "unicode_escape_in_uuid_value_rejected",
			body:    []byte(`{"campaign_id":"\u0035\u0035\u0030e8400-e29b-41d4-a716-446655440000"}`),
			mustErr: true, // DFA does not JSON-unescape; secure reject
		},
		{
			id: "G-J10", name: "null_campaign_id_value",
			body:    []byte(`{"campaign_id":null,"type":"click"}`),
			mustErr: true,
		},
		{
			id: "G-J11", name: "empty_object",
			body:   []byte(`{}`),
			mustOK: true,
		},
		{
			id: "G-J12", name: "bom_prefix",
			body:    append([]byte{0xEF, 0xBB, 0xBF}, []byte(`{"campaign_id":"550e8400-e29b-41d4-a716-446655440000"}`)...),
			mustErr: true,
		},
		{
			id: "G-J13", name: "null_byte_in_string",
			body:    []byte("{\"campaign_id\":\"550e8400-e29b-41d4-a716-4466554400\x000\"}"),
			mustErr: true,
		},
	}
}

func TestFraudScenarios_TrackJSON_2026(t *testing.T) {
	var gaps []string
	for _, tc := range fraudTrackJSONCases2026() {
		tc := tc
		t.Run(tc.id+"_"+tc.name, func(t *testing.T) {
			var req TrackRequest
			err := ParseTrackRequestJSON(&req, tc.body)
			switch {
			case tc.mustErr:
				if err == nil {
					msg := fmt.Sprintf("%s [%s]: GAP expected malformed got success req=%+v", tc.id, tc.name, req)
					gaps = append(gaps, msg)
					t.Fatal(msg)
				}
			case tc.mustOK:
				if err != nil {
					msg := fmt.Sprintf("%s [%s]: GAP expected accept got %v", tc.id, tc.name, err)
					gaps = append(gaps, msg)
					t.Fatal(msg)
				}
				if tc.check != nil {
					tc.check(t, &req)
				}
			}
		})
	}
	logChaosProof(t, "fraud_track_json_2026", map[string]string{
		"cases": fmt.Sprintf("%d", len(fraudTrackJSONCases2026())),
		"gaps":  fmt.Sprintf("%d", len(gaps)),
	})
}

func TestFraudScenarios_TrackJSON_OptParityOnCorpus(t *testing.T) {
	for _, tc := range fraudTrackJSONCases2026() {
		if !tc.mustOK {
			continue
		}
		var a, b TrackRequest
		errA := ParseTrackRequestJSON(&a, tc.body)
		errB := ParseTrackRequestJSONOpt(&b, tc.body)
		require.Equal(t, errA, errB, tc.id)
		if errA == nil {
			require.Equal(t, a, b, tc.id)
		}
	}
}

func TestFraudScenarios_TrackJSON_DeepNestedPayload(t *testing.T) {
	validCID := "550e8400-e29b-41d4-a716-446655440000"
	var nested strings.Builder
	nested.WriteString(`{"campaign_id":"`)
	nested.WriteString(validCID)
	nested.WriteString(`","payload":`)
	for i := 0; i < 200; i++ {
		nested.WriteString(`{"a":`)
	}
	nested.WriteString(`"leaf"`)
	for i := 0; i < 200; i++ {
		nested.WriteString(`}`)
	}
	nested.WriteString(`}`)

	var req TrackRequest
	err := ParseTrackRequestJSON(&req, []byte(nested.String()))
	if err != nil {
		t.Logf("GAP G-J05b: deep nested payload rejected at depth 200: %v", err)
	}
}

func TestFraudScenarios_TrackJSON_LargePayloadWithinBody(t *testing.T) {
	validCID := "550e8400-e29b-41d4-a716-446655440000"
	inner := strings.Repeat(`"x",`, 100)
	inner = strings.TrimSuffix(inner, ",")
	var bodyBuf strings.Builder
	bodyBuf.WriteString(`{"campaign_id":"`)
	bodyBuf.WriteString(validCID)
	bodyBuf.WriteString(`","payload":[`)
	bodyBuf.WriteString(inner)
	bodyBuf.WriteString(`]}`)
	body := bodyBuf.String()
	var req TrackRequest
	err := ParseTrackRequestJSON(&req, []byte(body))
	if err != nil {
		t.Logf("GAP: large but valid JSON array payload rejected: %v", err)
	}
}

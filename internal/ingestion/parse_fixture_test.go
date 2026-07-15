package ingestion

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testTrackRequestJSON(tb testing.TB) []byte {
	tb.Helper()

	campaignUUID := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	payload := struct {
		CampaignID string          `json:"campaign_id"`
		UserID     string          `json:"user_id"`
		Type       string          `json:"type"`
		ClickID    string          `json:"click_id"`
		Payload    json.RawMessage `json:"payload"`
	}{
		CampaignID: campaignUUID.String(),
		UserID:     "user_987654321",
		Type:       "click",
		ClickID:    "c12345",
		Payload:    json.RawMessage(`{"slot":"top","cpm":1.25}`),
	}

	data, err := json.Marshal(payload)
	require.NoError(tb, err)
	return data
}

// trackRequestReflect is the encoding/json baseline used only in benchmarks and parity tests.
type trackRequestReflect struct {
	CampaignID uuid.UUID       `json:"campaign_id"`
	UserID     string          `json:"user_id"`
	Type       string          `json:"type"`
	ClickID    string          `json:"click_id"`
	Payload    json.RawMessage `json:"payload"`
}

func resetTrackRequestReflect(v *trackRequestReflect) {
	v.CampaignID = uuid.Nil
	v.UserID = ""
	v.Type = ""
	v.ClickID = ""
	v.Payload = v.Payload[:0]
}

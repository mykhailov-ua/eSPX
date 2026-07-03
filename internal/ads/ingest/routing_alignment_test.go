package ingest

import (
	"encoding/json"
	"hash/crc32"
	"testing"

	"espx/internal/ads/pb"
	"espx/internal/ads/sharding"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// legacyCompositeHash is the string-concat reference for nginx alignment checks.
func legacyCompositeHash(campaignID, userID string) uint32 {
	if campaignID == "" && userID == "" {
		return 0
	}
	return crc32.ChecksumIEEE([]byte(campaignID + userID))
}

// Guards JSON and protobuf ingest compute the same composite routing hash.
func TestCompositeRouting_JSONAndProtoAlignment(t *testing.T) {
	campaignUUID := uuid.New()
	userID := "user_987654321"

	jsonPayload := struct {
		CampaignID string          `json:"campaign_id"`
		UserID     string          `json:"user_id"`
		Type       string          `json:"type"`
		ClickID    string          `json:"click_id"`
		Payload    json.RawMessage `json:"payload"`
	}{
		CampaignID: campaignUUID.String(),
		UserID:     userID,
		Type:       "click",
		ClickID:    "c12345",
		Payload:    json.RawMessage(`{}`),
	}
	jsonData, err := json.Marshal(jsonPayload)
	require.NoError(t, err)

	var trackReq TrackRequest
	err = ParseTrackRequestJSON(&trackReq, jsonData)
	require.NoError(t, err)

	legacyHash := legacyCompositeHash(trackReq.CampaignID.String(), trackReq.UserID)
	jsonHash := ComputeCompositeHashFromTrackReq(&trackReq)
	require.Equal(t, legacyHash, jsonHash)

	pbReq := pb.AdEvent{
		CampaignId: campaignUUID[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("c12345"),
			UserId:  []byte(userID),
		},
	}
	protoData, err := pbReq.MarshalVT()
	require.NoError(t, err)

	reqProto := &pb.AdEvent{}
	err = reqProto.UnmarshalVT(protoData)
	require.NoError(t, err)

	protoCampaignUUID, err := uuid.FromBytes(reqProto.CampaignId)
	require.NoError(t, err)
	protoCampaignIDStr := protoCampaignUUID.String()
	protoUserIDStr := string(reqProto.Metadata.UserId)
	protoLegacyHash := legacyCompositeHash(protoCampaignIDStr, protoUserIDStr)
	protoHash := sharding.ComputeCompositeHashFromProto(reqProto)

	assert.Equal(t, trackReq.CampaignID.String(), protoCampaignIDStr)
	assert.Equal(t, trackReq.UserID, protoUserIDStr)
	assert.Equal(t, legacyHash, protoHash)
	assert.Equal(t, protoLegacyHash, protoHash)
}

// Tracks JSON composite hash cost against protobuf for format migration decisions.
func BenchmarkCompositeRouting_JSON(b *testing.B) {
	campaignUUID := uuid.New()
	userID := "user_987654321"

	jsonPayload := struct {
		CampaignID string          `json:"campaign_id"`
		UserID     string          `json:"user_id"`
		Type       string          `json:"type"`
		ClickID    string          `json:"click_id"`
		Payload    json.RawMessage `json:"payload"`
	}{
		CampaignID: campaignUUID.String(),
		UserID:     userID,
		Type:       "click",
		ClickID:    "c12345",
		Payload:    json.RawMessage(`{}`),
	}
	jsonData, _ := json.Marshal(jsonPayload)

	var trackReq TrackRequest

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		trackReq.Reset()
		_ = ParseTrackRequestJSON(&trackReq, jsonData)
		_ = ComputeCompositeHashFromTrackReq(&trackReq)
	}
}

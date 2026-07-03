package ads

import (
	"encoding/json"
	"hash/crc32"
	"testing"

	"espx/internal/ads/pb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ComputeCompositeHash is the legacy string-concat reference for nginx alignment checks.
func ComputeCompositeHash(campaignID, userID string) uint32 {
	if campaignID == "" && userID == "" {
		return 0
	}
	key := campaignID + userID
	return crc32.ChecksumIEEE([]byte(key))
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

	legacyHash := ComputeCompositeHash(trackReq.CampaignID.String(), trackReq.UserID)
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

	reqProto := adEventPool.Get().(*pb.AdEvent)
	defer putAdEvent(reqProto)

	err = reqProto.UnmarshalVT(protoData)
	require.NoError(t, err)

	protoCampaignUUID, err := uuid.FromBytes(reqProto.CampaignId)
	require.NoError(t, err)
	protoCampaignIDStr := protoCampaignUUID.String()
	protoUserIDStr := string(reqProto.Metadata.UserId)
	protoLegacyHash := ComputeCompositeHash(protoCampaignIDStr, protoUserIDStr)
	protoHash := ComputeCompositeHashFromProto(reqProto)

	assert.Equal(t, trackReq.CampaignID.String(), protoCampaignIDStr)
	assert.Equal(t, trackReq.UserID, protoUserIDStr)
	assert.Equal(t, legacyHash, protoHash)
	assert.Equal(t, protoLegacyHash, protoHash)

	t.Logf("Aligned Campaign ID: %s", protoCampaignIDStr)
	t.Logf("Aligned User ID: %s", protoUserIDStr)
	t.Logf("Composite Routing Hash (CRC32): %d (0x%x)", jsonHash, jsonHash)
}

func TestComputeCompositeHashUUID_ZeroAlloc(t *testing.T) {
	var campaignUUID uuid.UUID
	copy(campaignUUID[:], []byte{0x6d, 0x94, 0x24, 0x9c, 0xa9, 0xb9, 0x4a, 0x73, 0x9c, 0x96, 0x30, 0xc4, 0x3c, 0x34, 0xbc, 0x3e})
	userID := UnsafeBytes("user_987654321")

	avg := testing.AllocsPerRun(100, func() {
		_ = ComputeCompositeHashUUID(campaignUUID, userID)
	})
	if avg > 0 {
		t.Fatalf("ComputeCompositeHashUUID allocated %f times per run, want 0", avg)
	}
}

func TestFormatUUIDCanonical(t *testing.T) {
	id := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	var buf [36]byte
	FormatUUIDCanonical(&buf, id)
	require.Equal(t, id.String(), string(buf[:]))

	var buf2 [36]byte
	FormatUUIDCanonical(&buf2, id)
	require.Equal(t, crc32.ChecksumIEEE(buf[:]), crc32IEEEInplace36(&buf2))
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

// Tracks protobuf composite hash as baseline for routing hot path.
func BenchmarkCompositeRouting_Protobuf(b *testing.B) {
	campaignUUID := uuid.New()
	userID := "user_987654321"

	pbReq := pb.AdEvent{
		CampaignId: campaignUUID[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("c12345"),
			UserId:  []byte(userID),
		},
	}
	protoData, _ := pbReq.MarshalVT()

	reqProto := adEventPool.Get().(*pb.AdEvent)
	defer putAdEvent(reqProto)

	// Warm up pool allocations before timing.
	resetAdEventInPlace(reqProto)
	_ = reqProto.UnmarshalVT(protoData)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resetAdEventInPlace(reqProto)
		_ = reqProto.UnmarshalVT(protoData)
		_ = ComputeCompositeHashFromProto(reqProto)
	}
}

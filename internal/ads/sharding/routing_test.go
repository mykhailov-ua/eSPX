package sharding

import (
	"hash/crc32"
	"testing"

	"espx/internal/ads/pb"

	"github.com/google/uuid"
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

func TestComputeCompositeHashUUID_ZeroAlloc(t *testing.T) {
	var campaignUUID uuid.UUID
	copy(campaignUUID[:], []byte{0x6d, 0x94, 0x24, 0x9c, 0xa9, 0xb9, 0x4a, 0x73, 0x9c, 0x96, 0x30, 0xc4, 0x3c, 0x34, 0xbc, 0x3e})
	userID := []byte("user_987654321")

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

	reqProto := &pb.AdEvent{}

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

package ingestion

import (
	"encoding/json"
	"testing"

	"espx/internal/ingestion/pb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestParseTrackRequestJSON(t *testing.T) {
	data := testTrackRequestJSON(t)

	var reqReflect trackRequestReflect
	err := json.Unmarshal(data, &reqReflect)
	require.NoError(t, err)

	var req TrackRequest
	err = ParseTrackRequestJSON(&req, data)
	require.NoError(t, err)

	require.Equal(t, reqReflect.CampaignID, req.CampaignID)
	require.Equal(t, reqReflect.UserID, req.UserID)
	require.Equal(t, reqReflect.Type, req.Type)
	require.Equal(t, reqReflect.ClickID, req.ClickID)
	require.JSONEq(t, string(reqReflect.Payload), string(req.Payload))
}

func TestParseTrackRequestJSONOptParity(t *testing.T) {
	data := testTrackRequestJSON(t)

	var req TrackRequest
	require.NoError(t, ParseTrackRequestJSON(&req, data))

	var reqOpt TrackRequest
	require.NoError(t, ParseTrackRequestJSONOpt(&reqOpt, data))

	require.Equal(t, req, reqOpt)
}

func TestParseTrackRequestJSONOpt_ZeroAlloc(t *testing.T) {
	data := testTrackRequestJSON(t)
	var req TrackRequest

	avg := testing.AllocsPerRun(100, func() {
		req.Reset()
		if err := ParseTrackRequestJSONOpt(&req, data); err != nil {
			t.Fatal(err)
		}
	})
	if avg > 0 {
		t.Fatalf("ParseTrackRequestJSONOpt allocated %f times per run, want 0", avg)
	}
}

func TestParseTrackRequestJSON_ZeroAlloc(t *testing.T) {
	data := testTrackRequestJSON(t)
	var req TrackRequest

	avg := testing.AllocsPerRun(100, func() {
		req.Reset()
		if err := ParseTrackRequestJSON(&req, data); err != nil {
			t.Fatal(err)
		}
	})
	if avg > 0 {
		t.Fatalf("ParseTrackRequestJSON allocated %f times per run, want 0", avg)
	}
}

func testProtoTrackBody(t testing.TB) []byte {
	t.Helper()
	id := uuid.New()
	evt := &pb.AdEvent{
		CampaignId: id[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("test-click"),
			UserId:  []byte("user123"),
		},
	}
	body, err := evt.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func resetPooledAdEvent(evt *pb.AdEvent) {
	evt.CampaignId = evt.CampaignId[:0]
	evt.EventType = evt.EventType[:0]
	if evt.Metadata == nil {
		evt.Metadata = &pb.EventMetadata{}
		return
	}
	md := evt.Metadata
	md.ClickId = md.ClickId[:0]
	md.UserId = md.UserId[:0]
	md.DeviceType = md.DeviceType[:0]
	md.Os = md.Os[:0]
	for i := range md.ExtraKeys {
		md.ExtraKeys[i] = md.ExtraKeys[i][:0]
	}
	md.ExtraKeys = md.ExtraKeys[:0]
	for i := range md.ExtraValues {
		md.ExtraValues[i] = md.ExtraValues[i][:0]
	}
	md.ExtraValues = md.ExtraValues[:0]
	md.ExtraBytes = md.ExtraBytes[:0]
}

func TestAdEvent_UnmarshalVT_ZeroAlloc(t *testing.T) {
	body := testProtoTrackBody(t)
	var evt pb.AdEvent
	evt.Metadata = &pb.EventMetadata{}

	for i := 0; i < 100; i++ {
		resetPooledAdEvent(&evt)
		if err := evt.UnmarshalVT(body); err != nil {
			t.Fatal(err)
		}
	}

	avg := testing.AllocsPerRun(100, func() {
		resetPooledAdEvent(&evt)
		if err := evt.UnmarshalVT(body); err != nil {
			t.Fatal(err)
		}
	})
	if avg > 0 {
		t.Fatalf("AdEvent.UnmarshalVT allocated %f times per run, want 0", avg)
	}
}

func testProtoTrackBodyExtraRepeated(t testing.TB) []byte {
	t.Helper()
	id := uuid.New()
	evt := &pb.AdEvent{
		CampaignId: id[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId:     []byte("test-click"),
			UserId:      []byte("user123"),
			ExtraKeys:   [][]byte{[]byte("slot"), []byte("cpm")},
			ExtraValues: [][]byte{[]byte("top"), []byte("1.25")},
		},
	}
	body, err := evt.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestAdEvent_UnmarshalVT_ExtraRepeated_ZeroAlloc(t *testing.T) {
	body := testProtoTrackBodyExtraRepeated(t)
	var evt pb.AdEvent
	evt.Metadata = &pb.EventMetadata{}

	for i := 0; i < 100; i++ {
		resetPooledAdEvent(&evt)
		if err := evt.UnmarshalVT(body); err != nil {
			t.Fatal(err)
		}
	}

	avg := testing.AllocsPerRun(100, func() {
		resetPooledAdEvent(&evt)
		if err := evt.UnmarshalVT(body); err != nil {
			t.Fatal(err)
		}
	})
	if avg > 0 {
		t.Fatalf("AdEvent.UnmarshalVT extra repeated allocated %f times per run, want 0", avg)
	}
}

func BenchmarkTrackRequest_ParseJSON(b *testing.B) {
	data := testTrackRequestJSON(b)
	var req TrackRequest

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req.Reset()
		if err := ParseTrackRequestJSON(&req, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrackRequest_ParseJSONOpt(b *testing.B) {
	data := testTrackRequestJSON(b)
	var req TrackRequest

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req.Reset()
		if err := ParseTrackRequestJSONOpt(&req, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrackRequest_Unmarshal_Reflect(b *testing.B) {
	data := testTrackRequestJSON(b)

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var req trackRequestReflect
		resetTrackRequestReflect(&req)
		if err := json.Unmarshal(data, &req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrackRequest_UnmarshalJSON(b *testing.B) {
	data := testTrackRequestJSON(b)

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var req TrackRequest
		req.Reset()
		if err := req.UnmarshalJSON(data); err != nil {
			b.Fatal(err)
		}
	}
}

func TestParseUUID(t *testing.T) {
	// Standard 36-byte hex format
	id := uuid.New()
	idStr := id.String()
	idBytes := []byte(idStr)

	var got uuid.UUID
	ok := ParseUUID(idBytes, &got)
	require.True(t, ok)
	require.Equal(t, id, got)

	// Raw 16-byte format
	idRaw := id[:]
	var gotRaw uuid.UUID
	ok = ParseUUID(idRaw, &gotRaw)
	require.True(t, ok)
	require.Equal(t, id, gotRaw)

	// Invalid inputs
	require.False(t, ParseUUID([]byte("invalid-uuid-length-not-36"), &got))
	require.False(t, ParseUUID([]byte("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1g"), &got)) // invalid char 'g'
}

func BenchmarkUUID_ParseBytes_Reflect(b *testing.B) {
	id := uuid.New()
	idBytes := []byte(id.String())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		_, err = uuid.ParseBytes(idBytes)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUUID_ParseUUID_Custom(b *testing.B) {
	id := uuid.New()
	idBytes := []byte(id.String())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var got uuid.UUID
		if !ParseUUID(idBytes, &got) {
			b.Fatal("failed to parse")
		}
	}
}

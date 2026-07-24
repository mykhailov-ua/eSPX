package ingestion

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var openrtb26Sample = []byte(`{
  "id":"req-1",
  "tmax":250,
  "imp":[{"id":"1","bidfloor":1.25,"pmp":{"deals":[{"id":"deal-a"}]}}],
  "device":{"devicetype":1},
  "site":{"cat":["IAB1"]}
}`)

func TestParseOpenRTB26_fields(t *testing.T) {
	p := ParseOpenRTB26(openrtb26Sample)
	require.True(t, p.OK)
	assert.Equal(t, int64(1_250_000), p.BidFloorMicro)
	assert.Equal(t, uint8(2), p.DeviceType)
	assert.Equal(t, int32(250), p.TmaxMs)
	assert.Equal(t, uint8(1), p.SeatCount)
	assert.Equal(t, "deal-a", string(p.DealID[:p.DealIDLen]))
}

func BenchmarkParseOpenRTB26(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ParseOpenRTB26(openrtb26Sample)
	}
}

func BenchmarkWriteOpenRTB26BidHTTP(b *testing.B) {
	id := []byte("req-1")
	camp := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	var buf [768]byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = writeOpenRTB26BidHTTP(buf[:], id, 1_250_000, camp)
	}
}

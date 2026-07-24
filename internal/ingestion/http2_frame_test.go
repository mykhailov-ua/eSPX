package ingestion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP2DecodeFrameHeader(t *testing.T) {
	buf := []byte{0x00, 0x00, 0x05, h2FrameData, 0x00, 0x00, 0x00, 0x00, 0x01, 'h', 'e', 'l', 'l', 'o'}
	fr, n, err := decodeH2FrameHeader(buf)
	require.NoError(t, err)
	assert.Equal(t, 14, n)
	assert.Equal(t, uint32(5), fr.Length)
	assert.Equal(t, h2FrameData, fr.Type)
	assert.Equal(t, uint32(1), fr.StreamID)
	assert.Equal(t, []byte("hello"), fr.Payload)
}

func TestHTTP2DecodeFrameHeader_ZeroAlloc(t *testing.T) {
	buf := []byte{0x00, 0x00, 0x05, h2FrameData, 0x00, 0x00, 0x00, 0x00, 0x01, 'h', 'e', 'l', 'l', 'o'}
	allocs := testing.AllocsPerRun(100, func() {
		_, _, err := decodeH2FrameHeader(buf)
		if err != nil {
			t.Fatal(err)
		}
	})
	assert.Equal(t, float64(0), allocs)
}

func TestHTTP2HpackStaticPostTrack(t *testing.T) {
	var req parsedHTTPRequest
	block := []byte{
		0x83,                                     // :method POST (index 3)
		0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k', // :path /track (name index 4, literal value)
	}
	require.NoError(t, h2DecodeHeadersBlock(block, &req))
	assert.Equal(t, "POST", string(req.Method))
	assert.Equal(t, "/track", string(req.Path))
}

func BenchmarkHTTP2DecodeFrame(b *testing.B) {
	buf := []byte{0x00, 0x00, 0x05, h2FrameData, 0x00, 0x00, 0x00, 0x00, 0x01, 'h', 'e', 'l', 'l', 'o'}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := decodeH2FrameHeader(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

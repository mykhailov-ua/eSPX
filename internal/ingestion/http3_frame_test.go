package ingestion

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP3VarintDecode(t *testing.T) {
	v, n, err := quicDecodeVarint([]byte{0x25}, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, uint64(37), v)

	v, n, err = quicDecodeVarint([]byte{0x7b, 0xbd}, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, uint64(15293), v)
}

func TestHTTP3ParseRequestFrames(t *testing.T) {
	body := []byte(`{}`)
	var hdrBlock []byte
	hdrBlock = append(hdrBlock, 0x83, 0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k')
	var buf bytes.Buffer
	writeHTTP3Frame(&buf, h3FrameHeaders, hdrBlock)
	writeHTTP3Frame(&buf, h3FrameData, body)
	consumed, req, err := h3ParseRequestFrames(buf.Bytes(), 1<<20)
	require.NoError(t, err)
	assert.Equal(t, buf.Len(), consumed)
	assert.Equal(t, "/track", string(req.Path))
	assert.Equal(t, body, req.Body)
}

func writeHTTP3Frame(buf *bytes.Buffer, typ uint64, payload []byte) {
	encodeHTTP3Varint(buf, typ)
	encodeHTTP3Varint(buf, uint64(len(payload)))
	buf.Write(payload)
}

func encodeHTTP3Varint(buf *bytes.Buffer, v uint64) {
	if v < 0x40 {
		buf.WriteByte(byte(v))
		return
	}
	if v < 0x4000 {
		buf.WriteByte(byte(0x40 | (v>>8)&0x3f))
		buf.WriteByte(byte(v))
		return
	}
	buf.WriteByte(0x80)
	buf.WriteByte(byte(v >> 8))
}

func BenchmarkHTTP3VarintDecode(b *testing.B) {
	buf := []byte{0x7b, 0xbd}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := quicDecodeVarint(buf, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

package ingestion

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildH2TrackRequest(body []byte) []byte {
	// :method POST (3), :path /track (literal name idx 4)
	hdrBlock := []byte{0x83, 0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k'}
	if len(body) > 0 {
		hdrBlock = append(hdrBlock, 0x9f) // content-type: application/json (static index 31)
	}

	var headersFrame []byte
	hdr := make([]byte, 9)
	encodeH2FrameHeader(hdr, uint32(len(hdrBlock)), h2FrameHeaders, h2FlagEndHeaders, 1)
	headersFrame = append(headersFrame, hdr...)
	headersFrame = append(headersFrame, hdrBlock...)

	var dataFrame []byte
	dh := make([]byte, 9)
	encodeH2FrameHeader(dh, uint32(len(body)), h2FrameData, h2FlagEndStream, 1)
	dataFrame = append(dataFrame, dh...)
	dataFrame = append(dataFrame, body...)

	var settings []byte
	settings = append(settings, []byte{
		0x00, 0x00, 0x00, h2FrameSettings, 0x00, 0x00, 0x00, 0x00, 0x00,
	}...)

	var buf bytes.Buffer
	buf.WriteString("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
	buf.Write(settings)
	buf.Write(headersFrame)
	buf.Write(dataFrame)
	return buf.Bytes()
}

func TestHTTP2IngressParseTrack(t *testing.T) {
	body := []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click"}`)
	wire := buildH2TrackRequest(body)
	st := newH2ConnState()
	consumed, req, streamID, settings, err := parseH2Ingress(wire, &st, 1<<20)
	require.NoError(t, err)
	assert.Greater(t, consumed, h2ClientPrefaceLen)
	assert.NotEmpty(t, settings)
	assert.Equal(t, uint32(1), streamID)
	assert.Equal(t, "POST", string(req.Method))
	assert.Equal(t, "/track", string(req.Path))
	assert.Equal(t, body, req.Body)
}

func TestHTTP2WrapResponse202(t *testing.T) {
	h1 := []byte("HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
	dst := make([]byte, 512)
	n, err := h2WrapH1Response(dst, 1, h1)
	require.NoError(t, err)
	assert.Greater(t, n, h2FrameHeaderSize)
}

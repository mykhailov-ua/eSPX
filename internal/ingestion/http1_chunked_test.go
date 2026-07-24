package ingestion

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP1ChunkedBody(t *testing.T) {
	const maxBody = int64(1024 * 1024)
	body := []byte(`{"campaign_id":"x","type":"click"}`)
	chunked := append([]byte(
		"POST /track HTTP/1.1\r\n"+
			"Transfer-Encoding: chunked\r\n"+
			"Content-Type: application/json\r\n"+
			"\r\n"),
		[]byte(fmt.Sprintf("%x\r\n", len(body)))...)
	chunked = append(chunked, body...)
	chunked = append(chunked, "\r\n0\r\n\r\n"...)
	n, req, err := parseHTTP1(chunked, maxBody)
	require.NoError(t, err)
	assert.Equal(t, len(chunked), n)
	assert.Equal(t, body, req.Body)
	assert.Equal(t, len(body), req.ContentLength)
}

func TestHTTP1ChunkedEmpty(t *testing.T) {
	payload := []byte("POST /track HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n")
	n, req, err := parseHTTP1(payload, 1024)
	require.NoError(t, err)
	assert.Equal(t, len(payload), n)
	assert.Empty(t, req.Body)
}

func TestHTTP1ChunkedRejectCLCombo(t *testing.T) {
	payload := []byte("POST /track HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n0\r\n\r\n")
	_, _, err := parseHTTP1(payload, 1024)
	assert.ErrorIs(t, err, errInvalidRequest)
}

package ingestion

import (
	"net/http"
	"testing"

	"espx/internal/config"

	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTP1Parse_IncompleteTwoReads verifies split TCP delivery across two OnTraffic calls.
func TestHTTP1Parse_IncompleteTwoReads(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click"}`)
	req := BuildGnetPostTrackJSON(body)
	split := len(req) / 2

	conn := NewGnetHarnessConn(req[:split])
	assert.Equal(t, gnet.None, h.OnTraffic(conn))
	assert.NotEmpty(t, conn.inbound, "incomplete request must remain buffered")

	conn.inbound = append(conn.inbound[:0], req...)
	assert.Equal(t, gnet.None, h.OnTraffic(conn))
	assert.Equal(t, 1, conn.WriteCount())
	assert.Equal(t, http.StatusAccepted, ParseGnetHTTPStatus(conn.Written()))
}

// TestHTTP1Parse_SplitAtHeaderBoundary splits between headers and body.
func TestHTTP1Parse_SplitAtHeaderBoundary(t *testing.T) {
	const maxBody = int64(1024 * 1024)
	full := []byte("POST /track HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello")
	n, _, err := parseHTTP1(full[:len(full)-3], maxBody)
	require.ErrorIs(t, err, errIncompleteRequest)
	assert.Equal(t, 0, n)

	n, req, err := parseHTTP1(full, maxBody)
	require.NoError(t, err)
	assert.Equal(t, len(full), n)
	assert.Equal(t, "hello", string(req.Body))
}

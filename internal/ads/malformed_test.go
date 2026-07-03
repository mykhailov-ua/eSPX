package ads

import (
	"bytes"
	"net/http"
	"testing"

	"espx/internal/config"
	"github.com/stretchr/testify/assert"
)

// Guards malformed protobuf, oversize body, and bad campaign ID return 400 not 500 via OnTraffic.
func TestTrackHandlerMalformed(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	handler := NewAdsPacketHandler(cfg, registry, nil, nil, nil, sharder, "fraud-stream", nil)

	t.Run("Malformed Protobuf", func(t *testing.T) {
		body := []byte{0xFF, 0xEE, 0xDD}
		status, written := PostTrackGnet(handler, body, "application/x-protobuf", "")
		assert.Equal(t, http.StatusBadRequest, status)
		assert.True(t, bytes.HasPrefix(written, []byte("HTTP/1.1 400")))
	})

	t.Run("Payload Too Large", func(t *testing.T) {
		body := make([]byte, 2048)
		status, _ := PostTrackGnet(handler, body, "application/x-protobuf", "")
		assert.Equal(t, http.StatusRequestEntityTooLarge, status)
	})

	t.Run("Invalid db.Campaign ID", func(t *testing.T) {
		body := []byte{10, 3, 104, 105, 33}
		status, _ := PostTrackGnet(handler, body, "application/x-protobuf", "")
		assert.Equal(t, http.StatusBadRequest, status)
	})
}

package ingestion

import (
	"bytes"
	"net/http"
	"testing"

	"espx/internal/config"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdsPacketHandler_UDPIngress_429 returns 429 when per-worker ingress quota is exhausted.
func TestAdsPacketHandler_UDPIngress_429(t *testing.T) {
	before := testutil.ToFloat64(metrics.UDPIngressRejectTotal)

	udp := NewUDPControl(UDPControlConfig{
		Enabled:    true,
		NumShards:  4,
		NumWorkers: 1,
		InitialRPS: 1,
	})
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewStaticSlotSharder(4), "fraud", nil)
	h.SetUDPControl(udp)

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)

	status, _ := PostTrackGnetJSON(h, body)
	require.Equal(t, http.StatusAccepted, status)

	status, written := PostTrackGnetJSON(h, body)
	assert.Equal(t, http.StatusTooManyRequests, status)
	assert.True(t, bytes.HasPrefix(written, []byte("HTTP/1.1 429")))

	after := testutil.ToFloat64(metrics.UDPIngressRejectTotal)
	assert.Greater(t, after, before)
}

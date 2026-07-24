package ingestion

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/metrics"

	"github.com/panjf2000/gnet/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTrackRequestJSON_DepthCap(t *testing.T) {
	validCID := "550e8400-e29b-41d4-a716-446655440000"
	build := func(depth int) []byte {
		var b strings.Builder
		b.WriteString(`{"campaign_id":"`)
		b.WriteString(validCID)
		b.WriteString(`","payload":`)
		for i := 0; i < depth; i++ {
			b.WriteString(`{"a":`)
		}
		b.WriteString(`"leaf"`)
		for i := 0; i < depth; i++ {
			b.WriteString(`}`)
		}
		b.WriteString(`}`)
		return []byte(b.String())
	}

	var tr TrackRequest
	require.NoError(t, ParseTrackRequestJSON(&tr, build(MaxJSONDepth)))
	require.ErrorIs(t, ParseTrackRequestJSON(&tr, build(MaxJSONDepth+1)), ErrMalformed)
	require.ErrorIs(t, ParseTrackRequestJSON(&tr, build(1000)), ErrMalformed)
}

func TestParseTrackRequestJSON_Depth1000Under1us(t *testing.T) {
	validCID := "550e8400-e29b-41d4-a716-446655440000"
	var nested strings.Builder
	nested.WriteString(`{"campaign_id":"`)
	nested.WriteString(validCID)
	nested.WriteString(`","payload":`)
	for i := 0; i < 1000; i++ {
		nested.WriteString(`{"a":`)
	}
	nested.WriteString(`"leaf"`)
	for i := 0; i < 1000; i++ {
		nested.WriteString(`}`)
	}
	nested.WriteString(`}`)
	data := []byte(nested.String())

	var tr TrackRequest
	start := time.Now()
	err := ParseTrackRequestJSON(&tr, data)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrMalformed)
	assert.Less(t, elapsed, time.Microsecond, "depth-1000 reject took %v", elapsed)
}

func TestChaos_WireJSONDepthReject(t *testing.T) {
	validCID := "550e8400-e29b-41d4-a716-446655440000"
	var nested strings.Builder
	nested.WriteString(`{"campaign_id":"`)
	nested.WriteString(validCID)
	nested.WriteString(`","payload":`)
	for i := 0; i < 200; i++ {
		nested.WriteString(`{"a":`)
	}
	nested.WriteString(`"leaf"`)
	for i := 0; i < 200; i++ {
		nested.WriteString(`}`)
	}
	nested.WriteString(`}`)

	var tr TrackRequest
	parseErr := ParseTrackRequestJSON(&tr, []byte(nested.String()))
	require.Error(t, parseErr)
	require.True(t, errors.Is(parseErr, ErrMalformed) || errors.Is(parseErr, errMalformedJSON))

	logChaosProof(t, "wire_json_depth_reject", map[string]string{
		"depth": "200",
		"cap":   fmt.Sprintf("%d", MaxJSONDepth),
		"err":   parseErr.Error(),
	})
}

func TestChaos_H2HostileIncompleteDisconnect(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1 << 20, H2IncompleteMax: 3}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewStaticSlotSharder(1), "fraud", nil)
	defer func() { _ = h.Stop(nil) }()

	partial := append([]byte(nil), h2ClientPreface[:20]...)
	conn := NewGnetHarnessConn(partial)

	before := testutil.ToFloat64(metrics.H2HostileDisconnectTotal)
	var last gnet.Action
	for i := 0; i < 3; i++ {
		last = h.onTrafficH2(conn, partial)
	}
	assert.Equal(t, gnet.Close, last)
	assert.GreaterOrEqual(t, testutil.ToFloat64(metrics.H2HostileDisconnectTotal), before+1)

	logChaosProof(t, "h2_hostile_disconnect", map[string]string{
		"incomplete_max": "3",
		"action":         "close",
	})
}

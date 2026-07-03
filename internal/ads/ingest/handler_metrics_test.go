package ingest

import (
	"bytes"
	"testing"

	"espx/internal/ads/filter"
	"espx/internal/ads/pb"
	"espx/internal/ads/sharding"
	adstest "espx/internal/ads/testutil"
	"espx/internal/config"

	"github.com/google/uuid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Guards protobuf accept path increments prebound shard counters.
func TestAdsPacketHandler_preboundMetricsOnProtoAccept(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
		FilterTimeoutMs:    50,
	}
	registry := &adstest.MockRegistry{}
	sharder := sharding.NewJumpHashSharder(1)
	h := NewAdsPacketHandler(cfg, registry, nil, nil, nil, sharder, "fraud", nil)

	beforeProto := promtest.ToFloat64(h.trackMetrics.throughputProto)
	beforeAccepted := promtest.ToFloat64(h.trackMetrics.decisionAccepted)

	cid := uuid.New()
	pbPayload := &pb.AdEvent{
		CampaignId: cid[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("test-click"),
		},
	}
	body, err := proto.Marshal(pbPayload)
	require.NoError(t, err)

	req := parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/x-protobuf"),
		ClientIP:         []byte("1.1.1.1"),
		Body:             body,
		ContentLength:    len(body),
		HasContentLength: true,
	}
	conn := &mockGnetConn{written: make([]byte, 0, 512)}
	h.React(req, conn)

	require.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 202")))

	afterProto := promtest.ToFloat64(h.trackMetrics.throughputProto)
	afterAccepted := promtest.ToFloat64(h.trackMetrics.decisionAccepted)

	require.Equal(t, beforeProto+1, afterProto)
	require.Equal(t, beforeAccepted+1, afterAccepted)
}

// Guards reject path increments prebound counters with reason label.
func TestAdsPacketHandler_preboundMetricsOnReject(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
		FilterTimeoutMs:    50,
	}
	registry := &adstest.MockRegistry{}
	sharder := sharding.NewJumpHashSharder(1)
	h := NewAdsPacketHandler(cfg, registry, filter.NewFilterEngine(0, &errFilter{err: filter.ErrCampaignNotFound}), nil, nil, sharder, "fraud", nil)

	beforeBlocked := promtest.ToFloat64(h.trackMetrics.blockedCampaignNotFound)
	beforeDecision := promtest.ToFloat64(h.trackMetrics.decisionCampaignNotFound)

	payload := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	req := parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/json"),
		Body:             payload,
		ContentLength:    len(payload),
		HasContentLength: true,
	}
	conn := &mockGnetConn{written: make([]byte, 0, 512)}
	h.React(req, conn)

	afterBlocked := promtest.ToFloat64(h.trackMetrics.blockedCampaignNotFound)
	afterDecision := promtest.ToFloat64(h.trackMetrics.decisionCampaignNotFound)

	require.Equal(t, beforeBlocked+1, afterBlocked)
	require.Equal(t, beforeDecision+1, afterDecision)
}

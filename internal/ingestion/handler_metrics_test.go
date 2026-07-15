package ingestion

import (
	"bytes"
	"testing"

	"espx/internal/config"
	"espx/internal/ingestion/pb"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestAdsPacketHandler_preboundMetricsOnProtoAccept(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
		FilterTimeoutMs:    50,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	h := NewAdsPacketHandler(cfg, registry, nil, nil, nil, sharder, "fraud", nil)

	beforeProto := testutil.ToFloat64(h.trackMetrics.throughputProto)
	beforeAccepted := testutil.ToFloat64(h.trackMetrics.decisionAccepted)

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

	afterProto := testutil.ToFloat64(h.trackMetrics.throughputProto)
	afterAccepted := testutil.ToFloat64(h.trackMetrics.decisionAccepted)

	require.Equal(t, beforeProto+1, afterProto)
	require.Equal(t, beforeAccepted+1, afterAccepted)
}

func TestAdsPacketHandler_preboundMetricsOnReject(t *testing.T) {
	cfg := &config.Config{
		MaxRequestBodySize: 1024 * 1024,
		FilterTimeoutMs:    50,
	}
	registry := &mockRegistry{}
	sharder := NewJumpHashSharder(1)
	h := NewAdsPacketHandler(cfg, registry, NewFilterEngine(0, &errFilter{err: ErrCampaignNotFound}), nil, nil, sharder, "fraud", nil)

	beforeBlocked := testutil.ToFloat64(h.trackMetrics.blockedCampaignNotFound)
	beforeDecision := testutil.ToFloat64(h.trackMetrics.decisionCampaignNotFound)

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

	afterBlocked := testutil.ToFloat64(h.trackMetrics.blockedCampaignNotFound)
	afterDecision := testutil.ToFloat64(h.trackMetrics.decisionCampaignNotFound)

	require.Equal(t, beforeBlocked+1, afterBlocked)
	require.Equal(t, beforeDecision+1, afterDecision)
}

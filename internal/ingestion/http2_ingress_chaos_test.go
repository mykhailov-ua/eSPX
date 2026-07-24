package ingestion

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"espx/internal/config"

	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type http2ChaosCase struct {
	name    string
	payload []byte
	maxBody int64
	wantOK  bool
	wantErr error
}

func http2ChaosMalformedCases() []http2ChaosCase {
	const maxBody = int64(1024 * 1024)
	validBody := []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click"}`)
	validTrack := buildH2TrackRequest(validBody)

	return []http2ChaosCase{
		{name: "incomplete_preface", payload: h2ClientPreface[:20], maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "garbage_no_preface", payload: randomWireGarbage(64), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "preface_only", payload: append([]byte(nil), h2ClientPreface[:]...), maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "oversized_frame_length", payload: buildH2WireAfterPreface([]byte{0xff, 0xff, 0xff, h2FrameData, 0x00, 0x00, 0x00, 0x00, 0x01}), maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "headers_stream_zero", payload: buildH2WireAfterPreface(buildH2Frame(0, h2FrameHeaders, h2FlagEndHeaders|h2FlagEndStream, []byte{0x83})), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "invalid_hpack_index", payload: buildH2WireAfterPreface(buildH2HeadersDataFrames(1, []byte{0xff}, nil)), maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "data_before_headers", payload: buildH2WireAfterPreface(buildH2Frame(1, h2FrameData, h2FlagEndStream, []byte("x"))), maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "payload_too_large", payload: buildH2OversizedDataFrame(maxBody + 1), maxBody: maxBody, wantErr: errPayloadTooLarge},
		{name: "valid_track", payload: validTrack, maxBody: maxBody, wantOK: true},
		{name: "settings_only", payload: append(append([]byte(nil), h2ClientPreface[:]...), buildH2Frame(0, h2FrameSettings, 0, nil)...), maxBody: maxBody, wantErr: errIncompleteRequest},
	}
}

func buildH2WireAfterPreface(frames []byte) []byte {
	var buf bytes.Buffer
	buf.Write(h2ClientPreface[:])
	buf.Write(frames)
	return buf.Bytes()
}

func buildH2Frame(streamID uint32, typ, flags byte, payload []byte) []byte {
	hdr := make([]byte, 9)
	encodeH2FrameHeader(hdr, uint32(len(payload)), typ, flags, streamID)
	out := append(hdr, payload...)
	return out
}

func buildH2HeadersDataFrames(streamID uint32, hdrBlock, body []byte) []byte {
	flags := byte(h2FlagEndHeaders)
	if len(body) == 0 {
		flags |= h2FlagEndStream
	}
	var out []byte
	out = append(out, buildH2Frame(streamID, h2FrameHeaders, flags, hdrBlock)...)
	if len(body) > 0 {
		out = append(out, buildH2Frame(streamID, h2FrameData, h2FlagEndStream, body)...)
	}
	return out
}

func buildH2OversizedDataFrame(bodyLen int64) []byte {
	body := bytes.Repeat([]byte("x"), int(bodyLen))
	hdrBlock := []byte{0x83, 0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k'}
	settings := buildH2Frame(0, h2FrameSettings, 0, nil)
	return buildH2WireAfterPreface(append(settings, buildH2HeadersDataFrames(1, hdrBlock, body)...))
}

// TestChaos_HTTP2_MalformedCorpus ensures parseH2Ingress never panics on hostile wire input.
func TestChaos_HTTP2_MalformedCorpus(t *testing.T) {
	var (
		okCount   int
		errCounts = map[string]int{}
	)
	for _, tc := range http2ChaosMalformedCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			st := newH2ConnState()
			_, _, _, _, err := parseH2Ingress(tc.payload, &st, tc.maxBody)
			if tc.wantOK {
				require.NoError(t, err)
				okCount++
				return
			}
			require.Error(t, err)
			if tc.wantErr != nil {
				assert.True(t, errors.Is(err, tc.wantErr) || errors.Is(err, errIncompleteRequest) || errors.Is(err, errInvalidRequest),
					"got %v", err)
				switch {
				case errors.Is(err, errIncompleteRequest):
					errCounts[errIncompleteRequest.Error()]++
				case errors.Is(err, errInvalidRequest):
					errCounts[errInvalidRequest.Error()]++
				case errors.Is(err, errPayloadTooLarge):
					errCounts[errPayloadTooLarge.Error()]++
				}
			}
		})
	}
	logChaosProof(t, "http2_malformed_corpus", map[string]string{
		"cases":      fmt.Sprintf("%d", len(http2ChaosMalformedCases())),
		"ok":         fmt.Sprintf("%d", okCount),
		"incomplete": fmt.Sprintf("%d", errCounts[errIncompleteRequest.Error()]),
		"invalid":    fmt.Sprintf("%d", errCounts[errInvalidRequest.Error()]),
		"too_large":  fmt.Sprintf("%d", errCounts[errPayloadTooLarge.Error()]),
	})
}

// TestChaos_HTTP2_OnTrafficHandler verifies gnet h2c handler responses on hostile and valid wire.
func TestChaos_HTTP2_OnTrafficHandler(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)

	validBody := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	validTrack := buildH2TrackRequest(validBody)

	cases := []struct {
		name         string
		payload      []byte
		wantClose    bool
		wantAccepted bool
		minWrites    int
	}{
		{
			name:         "valid_h2_track",
			payload:      validTrack,
			wantAccepted: true,
			minWrites:    2,
		},
		{
			name:      "oversize_body",
			payload:   buildH2OversizedDataFrame(5000),
			wantClose: true,
			minWrites: 1,
		},
		{
			name:      "rst_stream_after_preface",
			payload:   buildH2WireAfterPreface(buildH2Frame(1, h2FrameRSTStream, 0, []byte{0, 0, 0, 8})),
			wantClose: true,
			minWrites: 1,
		},
	}

	var accepted, rejected, incomplete int
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			conn := NewGnetHarnessConn(tc.payload)
			act := h.OnTraffic(conn)
			if tc.wantClose {
				assert.Equal(t, gnet.Close, act)
				rejected++
			} else {
				assert.Equal(t, gnet.None, act)
				assert.Zero(t, conn.InboundBuffered())
			}
			require.GreaterOrEqual(t, conn.WriteCount(), tc.minWrites)
			if tc.wantAccepted {
				assert.GreaterOrEqual(t, conn.WriteCount(), 2)
				accepted++
			} else if !tc.wantClose {
				incomplete++
			}
		})
	}
	logChaosProof(t, "http2_on_traffic_handler", map[string]string{
		"accepted":   fmt.Sprintf("%d", accepted),
		"rejected":   fmt.Sprintf("%d", rejected),
		"incomplete": fmt.Sprintf("%d", incomplete),
	})
}

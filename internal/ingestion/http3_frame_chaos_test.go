package ingestion

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type http3ChaosCase struct {
	name    string
	payload []byte
	maxBody int64
	wantOK  bool
	wantErr error
}

func buildH3TrackRequest(body []byte) []byte {
	hdrBlock := []byte{0x83, 0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k'}
	var buf bytes.Buffer
	writeHTTP3Frame(&buf, h3FrameHeaders, hdrBlock)
	writeHTTP3Frame(&buf, h3FrameData, body)
	return buf.Bytes()
}

func buildH3OversizedFrameLength() []byte {
	var buf bytes.Buffer
	// type=HEADERS (1), length=1<<20+1 (rejected by h3DecodeFrame)
	buf.WriteByte(0x01)
	buf.Write([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	return buf.Bytes()
}

func http3ChaosMalformedCases() []http3ChaosCase {
	const maxBody = int64(1024 * 1024)
	validBody := []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click"}`)
	validTrack := buildH3TrackRequest(validBody)

	return []http3ChaosCase{
		{name: "empty", payload: nil, maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "single_null", payload: []byte{0}, maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "incomplete_varint", payload: []byte{0x80}, maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "garbage_wire", payload: randomWireGarbage(48), maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "oversized_frame_length", payload: buildH3OversizedFrameLength(), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "data_before_headers", payload: func() []byte {
			var buf bytes.Buffer
			writeHTTP3Frame(&buf, h3FrameData, []byte("x"))
			return buf.Bytes()
		}(), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "invalid_qpack_index", payload: func() []byte {
			var buf bytes.Buffer
			writeHTTP3Frame(&buf, h3FrameHeaders, []byte{0xff})
			return buf.Bytes()
		}(), maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "payload_too_large", payload: func() []byte {
			const smallMax = int64(256)
			body := bytes.Repeat([]byte("x"), int(smallMax+1))
			hdrBlock := []byte{0x83, 0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k'}
			var buf bytes.Buffer
			writeHTTP3Frame(&buf, h3FrameHeaders, hdrBlock)
			writeHTTP3Frame(&buf, h3FrameData, body)
			return buf.Bytes()
		}(), maxBody: 256, wantErr: errPayloadTooLarge},
		{name: "headers_only", payload: func() []byte {
			var buf bytes.Buffer
			hdrBlock := []byte{0x83, 0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k'}
			writeHTTP3Frame(&buf, h3FrameHeaders, hdrBlock)
			return buf.Bytes()
		}(), maxBody: maxBody, wantOK: true},
		{name: "valid_track", payload: validTrack, maxBody: maxBody, wantOK: true},
		{name: "settings_then_headers", payload: func() []byte {
			var buf bytes.Buffer
			writeHTTP3Frame(&buf, h3FrameSettings, []byte{0x01, 0x02})
			hdrBlock := []byte{0x83, 0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k'}
			writeHTTP3Frame(&buf, h3FrameHeaders, hdrBlock)
			writeHTTP3Frame(&buf, h3FrameData, validBody)
			return buf.Bytes()
		}(), maxBody: maxBody, wantOK: true},
	}
}

// TestChaos_HTTP3_MalformedCorpus ensures h3ParseRequestFrames never panics on hostile wire.
func TestChaos_HTTP3_MalformedCorpus(t *testing.T) {
	var (
		okCount   int
		errCounts = map[string]int{}
	)
	for _, tc := range http3ChaosMalformedCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("h3ParseRequestFrames panicked: %v", r)
				}
			}()
			_, _, err := h3ParseRequestFrames(tc.payload, tc.maxBody)
			if tc.wantOK {
				require.NoError(t, err)
				okCount++
				return
			}
			require.Error(t, err)
			if tc.wantErr != nil {
				assert.True(t, errors.Is(err, tc.wantErr) || errors.Is(err, errIncompleteRequest) || errors.Is(err, errInvalidRequest),
					"got %v", err)
			}
			switch {
			case errors.Is(err, errIncompleteRequest):
				errCounts[errIncompleteRequest.Error()]++
			case errors.Is(err, errInvalidRequest):
				errCounts[errInvalidRequest.Error()]++
			case errors.Is(err, errPayloadTooLarge):
				errCounts[errPayloadTooLarge.Error()]++
			}
		})
	}
	logChaosProof(t, "http3_malformed_corpus", map[string]string{
		"cases":      fmt.Sprintf("%d", len(http3ChaosMalformedCases())),
		"ok":         fmt.Sprintf("%d", okCount),
		"incomplete": fmt.Sprintf("%d", errCounts[errIncompleteRequest.Error()]),
		"invalid":    fmt.Sprintf("%d", errCounts[errInvalidRequest.Error()]),
		"too_large":  fmt.Sprintf("%d", errCounts[errPayloadTooLarge.Error()]),
	})
}

// TestChaos_HTTP3_ConcurrentParse hammers h3ParseRequestFrames from many goroutines.
func TestChaos_HTTP3_ConcurrentParse(t *testing.T) {
	const (
		workers    = 24
		iterations = 200
	)
	cases := http3ChaosMalformedCases()
	valid := buildH3TrackRequest([]byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click"}`))

	var (
		panics    atomic.Uint64
		completed atomic.Uint64
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				payload := cases[(w+i)%len(cases)].payload
				if i%5 == 0 {
					payload = valid
				}
				func() {
					defer func() {
						if r := recover(); r != nil {
							panics.Add(1)
						}
					}()
					_, _, _ = h3ParseRequestFrames(payload, 1<<20)
					completed.Add(1)
				}()
			}
		}()
	}
	wg.Wait()

	require.Zero(t, panics.Load())
	logChaosProof(t, "http3_concurrent_parse", map[string]string{
		"workers":    fmt.Sprintf("%d", workers),
		"iterations": fmt.Sprintf("%d", iterations),
		"completed":  fmt.Sprintf("%d", completed.Load()),
		"panics":     fmt.Sprintf("%d", panics.Load()),
	})
}

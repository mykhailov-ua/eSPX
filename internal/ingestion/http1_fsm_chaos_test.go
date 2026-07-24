package ingestion

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/config"

	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// http1ChaosCase is one malformed or edge-case wire payload for parseHTTP1 chaos.
type http1ChaosCase struct {
	name    string
	payload []byte
	maxBody int64
	wantOK  bool
	wantErr error // when wantOK is false, must error; if set, must match this error type
}

func http1ChaosMalformedCases() []http1ChaosCase {
	const maxBody = int64(1024)
	return []http1ChaosCase{
		{name: "empty", payload: nil, maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "single_null", payload: []byte{0}, maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "binary_garbage", payload: randomWireGarbage(256), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "null_in_request_line", payload: []byte("POS\x00T /track HTTP/1.1\r\nContent-Length: 0\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "null_in_header_name", payload: []byte("POST /track HTTP/1.1\r\nX-Fo\x00o: bar\r\nContent-Length: 0\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "null_in_header_value", payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 0\r\nX-Test: a\x00b\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "lf_only_line_endings", payload: []byte("POST /track HTTP/1.1\nContent-Length: 0\n\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "cr_only_no_lf", payload: []byte("POST /track HTTP/1.1\rContent-Length: 0\r\r"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "header_line_no_colon", payload: []byte("POST /track HTTP/1.1\r\nBadHeader\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "colon_no_value", payload: []byte("POST /track HTTP/1.1\r\nContent-Length:\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "content_length_alpha", payload: []byte("POST /track HTTP/1.1\r\nContent-Length: abc\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "content_length_mixed", payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 12abc\r\n\r\nhello"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "content_length_huge", payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 999999999999999999\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "content_length_at_limit_plus_one", payload: append([]byte("POST /track HTTP/1.1\r\nContent-Length: 1025\r\n\r\n"), bytes.Repeat([]byte("x"), 1025)...), maxBody: maxBody, wantErr: errPayloadTooLarge},
		{name: "content_length_at_limit", payload: append([]byte("POST /track HTTP/1.1\r\nContent-Length: 1024\r\n\r\n"), bytes.Repeat([]byte("y"), 1024)...), maxBody: maxBody, wantOK: true},
		{name: "body_shorter_than_cl", payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 100\r\n\r\nshort"), maxBody: maxBody, wantErr: errIncompleteRequest},
		{name: "body_longer_than_cl", payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 3\r\n\r\nabcdef"), maxBody: maxBody, wantOK: true},
		{name: "double_crlf_early", payload: []byte("POST /track HTTP/1.1\r\n\r\njunk"), maxBody: maxBody, wantOK: true},
		{name: "http_09_style", payload: []byte("GET /track\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "method_overflow", payload: append(append([]byte(nil), bytes.Repeat([]byte("A"), 8192)...), []byte(" /track HTTP/1.1\r\nContent-Length: 0\r\n\r\n")...), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "path_null_byte", payload: []byte("POST /tra\x00ck HTTP/1.1\r\nContent-Length: 0\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "fast_path_corrupt_version", payload: []byte("POST /track HTTP/2.0\r\nContent-Length: 0\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "fast_path_corrupt_method", payload: []byte("POST /track HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n"), maxBody: maxBody, wantOK: true},
		{name: "utf8_header_value", payload: []byte("POST /track HTTP/1.1\r\nUser-Agent: тест\xFF\xFE\r\nContent-Length: 0\r\n\r\n"), maxBody: maxBody, wantOK: true},
		{name: "crlf_in_header_value", payload: []byte("POST /track HTTP/1.1\r\nX-Evil: foo\r\nbar\r\nContent-Length: 0\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "negative_looking_cl", payload: []byte("POST /track HTTP/1.1\r\nContent-Length: -1\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
		{name: "leading_zero_cl", payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 0005\r\n\r\nhello"), maxBody: maxBody, wantOK: true},
		{name: "pipelined_valid_then_garbage", payload: append(append([]byte(nil), nginxTrackCorpus...), randomWireGarbage(64)...), maxBody: 1024 * 1024, wantOK: true},
		{name: "chunked_empty_ok", payload: []byte("POST /track HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"), maxBody: maxBody, wantOK: true},
		{name: "control_chars_in_method", payload: []byte("PO\x01ST /track HTTP/1.1\r\nContent-Length: 0\r\n\r\n"), maxBody: maxBody, wantErr: errInvalidRequest},
	}
}

func randomWireGarbage(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// TestChaos_HTTP1_MalformedCorpus ensures parseHTTP1 never panics on hostile wire input.
func TestChaos_HTTP1_MalformedCorpus(t *testing.T) {
	var (
		okCount    int
		errCounts  = map[string]int{}
		panicCount atomic.Uint64
	)
	for _, tc := range http1ChaosMalformedCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					panicCount.Add(1)
					t.Fatalf("parseHTTP1 panicked: %v", r)
				}
			}()
			n, req, err := parseHTTP1(tc.payload, tc.maxBody)
			if tc.wantOK {
				require.NoError(t, err, "payload=%q", truncateBytes(tc.payload, 80))
				assert.Greater(t, n, 0)
				assert.LessOrEqual(t, n, len(tc.payload))
				if req.HasContentLength {
					assert.Len(t, req.Body, req.ContentLength)
				}
				okCount++
				return
			}
			require.Error(t, err)
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr, "payload=%q", truncateBytes(tc.payload, 80))
				errCounts[tc.wantErr.Error()]++
			}
		})
	}
	logChaosProof(t, "http1_malformed_corpus", map[string]string{
		"cases":      fmt.Sprintf("%d", len(http1ChaosMalformedCases())),
		"ok":         fmt.Sprintf("%d", okCount),
		"panics":     fmt.Sprintf("%d", panicCount.Load()),
		"incomplete": fmt.Sprintf("%d", errCounts[errIncompleteRequest.Error()]),
		"invalid":    fmt.Sprintf("%d", errCounts[errInvalidRequest.Error()]),
		"too_large":  fmt.Sprintf("%d", errCounts[errPayloadTooLarge.Error()]),
	})
}

// TestChaos_HTTP1_OnTrafficMalformed verifies gnet handler responses on hostile input.
func TestChaos_HTTP1_OnTrafficMalformed(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)

	cases := []struct {
		name       string
		payload    []byte
		wantStatus int
		wantClose  bool
	}{
		{
			name:       "oversize_cl",
			payload:    append([]byte("POST /track HTTP/1.1\r\nContent-Length: 5000\r\n\r\n"), bytes.Repeat([]byte("z"), 5000)...),
			wantStatus: http.StatusRequestEntityTooLarge,
			wantClose:  true,
		},
		{
			name:       "invalid_request_line",
			payload:    []byte("POST /track\r\n\r\n"),
			wantStatus: http.StatusBadRequest,
			wantClose:  true,
		},
		{
			name:       "binary_garbage",
			payload:    randomWireGarbage(128),
			wantStatus: 0,
			wantClose:  false,
		},
		{
			name:       "missing_cl_on_track",
			payload:    []byte("POST /track HTTP/1.1\r\nContent-Type: application/json\r\n\r\n{}"),
			wantStatus: http.StatusBadRequest,
			wantClose:  true,
		},
		{
			name: "valid_minimal",
			payload: BuildGnetPostTrackJSON([]byte(
				`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`,
			)),
			wantStatus: http.StatusAccepted,
			wantClose:  false,
		},
	}

	var accepted, rejected, incomplete int
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			conn := NewGnetHarnessConn(tc.payload)
			act := h.OnTraffic(conn)
			status := ParseGnetHTTPStatus(conn.Written())
			if tc.wantClose {
				assert.Equal(t, gnet.Close, act)
			}
			if tc.wantStatus != 0 {
				assert.Equal(t, tc.wantStatus, status)
			}
			switch {
			case status == http.StatusAccepted:
				accepted++
			case status == 0:
				incomplete++
			default:
				rejected++
			}
		})
	}
	logChaosProof(t, "http1_on_traffic_malformed", map[string]string{
		"accepted":   fmt.Sprintf("%d", accepted),
		"rejected":   fmt.Sprintf("%d", rejected),
		"incomplete": fmt.Sprintf("%d", incomplete),
	})
}

// TestChaos_HTTP1_ConcurrentParse hammers parseHTTP1 from many goroutines (run with -race).
func TestChaos_HTTP1_ConcurrentParse(t *testing.T) {
	const (
		workers    = 32
		iterations = 500
	)
	cases := http1ChaosMalformedCases()
	cases = append(cases, http1ChaosCase{
		name:    "valid_corpus",
		payload: nginxTrackCorpus,
		maxBody: 1024 * 1024,
		wantOK:  true,
	})

	var (
		panics atomic.Uint64
		ok     atomic.Uint64
		fail   atomic.Uint64
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				tc := cases[(seed*iterations+i)%len(cases)]
				func() {
					defer func() {
						if recover() != nil {
							panics.Add(1)
						}
					}()
					_, _, err := parseHTTP1(tc.payload, tc.maxBody)
					if err == nil {
						ok.Add(1)
					} else {
						fail.Add(1)
					}
				}()
			}
		}(w)
	}
	wg.Wait()

	require.Zero(t, panics.Load(), "concurrent parseHTTP1 must not panic")
	total := ok.Load() + fail.Load()
	require.Equal(t, uint64(workers*iterations), total)
	logChaosProof(t, "http1_concurrent_parse", map[string]string{
		"workers":    fmt.Sprintf("%d", workers),
		"iterations": fmt.Sprintf("%d", iterations),
		"total":      fmt.Sprintf("%d", total),
		"ok":         fmt.Sprintf("%d", ok.Load()),
		"fail":       fmt.Sprintf("%d", fail.Load()),
		"panics":     fmt.Sprintf("%d", panics.Load()),
	})
}

// TestChaos_HTTP1_ConcurrentOnTraffic runs parallel gnet connections with valid and hostile payloads.
func TestChaos_HTTP1_ConcurrentOnTraffic(t *testing.T) {
	const (
		workers   = 24
		perWorker = 50
		maxBody   = 1024 * 1024
	)
	cfg := &config.Config{MaxRequestBodySize: maxBody}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)

	validBody := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	validReq := BuildGnetPostTrackJSON(validBody)
	malformed := http1ChaosMalformedCases()

	var (
		panics   atomic.Uint64
		accepted atomic.Uint64
		badClose atomic.Uint64
		other    atomic.Uint64
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				var payload []byte
				if i%3 == 0 {
					payload = validReq
				} else {
					tc := malformed[(workerID*perWorker+i)%len(malformed)]
					payload = tc.payload
				}
				func() {
					defer func() {
						if recover() != nil {
							panics.Add(1)
						}
					}()
					conn := NewGnetHarnessConn(payload)
					act := h.OnTraffic(conn)
					status := ParseGnetHTTPStatus(conn.Written())
					switch {
					case status == http.StatusAccepted:
						accepted.Add(1)
					case act == gnet.Close && status == http.StatusBadRequest:
						badClose.Add(1)
					case act == gnet.Close && status == http.StatusRequestEntityTooLarge:
						badClose.Add(1)
					case status == 0:
						// incomplete — expected for truncated garbage
					default:
						other.Add(1)
					}
				}()
			}
		}(w)
	}
	wg.Wait()

	require.Zero(t, panics.Load())
	logChaosProof(t, "http1_concurrent_on_traffic", map[string]string{
		"workers":    fmt.Sprintf("%d", workers),
		"per_worker": fmt.Sprintf("%d", perWorker),
		"accepted":   fmt.Sprintf("%d", accepted.Load()),
		"rejected":   fmt.Sprintf("%d", badClose.Load()),
		"other":      fmt.Sprintf("%d", other.Load()),
		"panics":     fmt.Sprintf("%d", panics.Load()),
	})
}

// chaosGnetConn supports concurrent Append while OnTraffic reads (slow-client simulation).
type chaosGnetConn struct {
	gnet.Conn
	mu        sync.Mutex
	inbound   []byte
	written   []byte
	responses [][]byte
	ctx       any
	addr      net.Addr
}

func newChaosGnetConn() *chaosGnetConn {
	return &chaosGnetConn{
		written: make([]byte, 0, 512),
		addr:    gnetHarnessRemoteAddr,
	}
}

func (c *chaosGnetConn) Context() any     { return c.ctx }
func (c *chaosGnetConn) SetContext(v any) { c.ctx = v }

func (c *chaosGnetConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.written = append(c.written[:0], b...)
	cp := append([]byte(nil), b...)
	c.responses = append(c.responses, cp)
	return len(b), nil
}

func (c *chaosGnetConn) AsyncWrite(buf []byte, callback gnet.AsyncCallback) error {
	_, err := c.Write(buf)
	if callback != nil {
		_ = callback(c, nil)
	}
	return err
}

func (c *chaosGnetConn) InboundBuffered() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inbound)
}

func (c *chaosGnetConn) Peek(n int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n > len(c.inbound) {
		n = len(c.inbound)
	}
	return c.inbound[:n], nil
}

func (c *chaosGnetConn) Discard(n int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n > len(c.inbound) {
		n = len(c.inbound)
	}
	c.inbound = c.inbound[n:]
	return n, nil
}

func (c *chaosGnetConn) RemoteAddr() net.Addr { return c.addr }

func (c *chaosGnetConn) Append(b []byte) {
	c.mu.Lock()
	c.inbound = append(c.inbound, b...)
	c.mu.Unlock()
}

// TestChaos_HTTP1_IncrementalConcurrentWrite interleaves byte writes with OnTraffic reads.
func TestChaos_HTTP1_IncrementalConcurrentWrite(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)

	payloads := [][]byte{
		nginxTrackCorpus,
		BuildGnetPostTrackJSON([]byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"x"}`)),
		[]byte("POST /track HTTP/1.1\r\nContent-Length: 999\r\n\r\n"),
		randomWireGarbage(200),
	}

	const rounds = 20
	var panics atomic.Uint64
	var completed atomic.Uint64

	for r := 0; r < rounds; r++ {
		raw := payloads[r%len(payloads)]
		conn := newChaosGnetConn()
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			chunk := 1 + (r % 7)
			for i := 0; i < len(raw); i += chunk {
				end := i + chunk
				if end > len(raw) {
					end = len(raw)
				}
				conn.Append(raw[i:end])
				time.Sleep(time.Microsecond)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(deadline) {
				func() {
					defer func() {
						if recover() != nil {
							panics.Add(1)
						}
					}()
					_ = h.OnTraffic(conn)
				}()
				time.Sleep(10 * time.Microsecond)
			}
		}()

		wg.Wait()
		if ParseGnetHTTPStatus(conn.written) == http.StatusAccepted {
			completed.Add(1)
		}
	}

	require.Zero(t, panics.Load())
	logChaosProof(t, "http1_incremental_concurrent_write", map[string]string{
		"rounds":    fmt.Sprintf("%d", rounds),
		"completed": fmt.Sprintf("%d", completed.Load()),
		"panics":    fmt.Sprintf("%d", panics.Load()),
	})
}

// TestChaos_HTTP1_PipelinedMalformedMix pipelines valid POSTs with truncated hostile tails.
func TestChaos_HTTP1_PipelinedMalformedMix(t *testing.T) {
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)

	valid := BuildGnetPostTrackJSON([]byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"p1"}`))
	var buf []byte
	for i := 0; i < 5; i++ {
		buf = append(buf, valid...)
	}
	buf = append(buf, []byte("POST /track HTTP/1.1\r\nContent-Length: 99999\r\n\r\n")...)
	buf = append(buf, randomWireGarbage(32)...)

	conn := NewGnetHarnessConn(buf)
	act := h.OnTraffic(conn)
	assert.Equal(t, gnet.None, act)
	assert.Equal(t, 5, conn.WriteCount())
	for _, resp := range conn.AllResponses() {
		assert.Equal(t, http.StatusAccepted, ParseGnetHTTPStatus(resp))
	}

	remaining := buf
	for i := 0; i < 5; i++ {
		n, _, err := parseHTTP1(remaining, cfg.MaxRequestBodySize)
		require.NoError(t, err)
		remaining = remaining[n:]
	}
	_, _, err := parseHTTP1(remaining, cfg.MaxRequestBodySize)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errIncompleteRequest) || errors.Is(err, errPayloadTooLarge))

	logChaosProof(t, "http1_pipelined_malformed_mix", map[string]string{
		"accepted": "5",
		"tail_err": err.Error(),
	})
}

// TestChaos_HTTP1_PipelinedKeepAliveBudget pipelines 10 impression POSTs on one keep-alive
// connection through the real chaos ingest stack and asserts budget invariant (R5).
func TestChaos_HTTP1_PipelinedKeepAliveBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx := context.Background()
	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	stack := startAdsIngestStack(t, infra, "ads-chaos-http1-pipeline")
	defer stack.Close(t)

	const n = 10
	var pipelined []byte
	for i := 0; i < n; i++ {
		body := fmt.Sprintf(
			`{"campaign_id":"%s","type":"impression","click_id":"pipe-%d","user_id":"pipe-user"}`,
			stack.CampaignID, i,
		)
		pipelined = append(pipelined, BuildGnetPostTrackJSON([]byte(body))...)
	}

	conn := NewGnetHarnessConn(pipelined)
	act := stack.Handler.OnTraffic(conn)
	assert.Equal(t, gnet.None, act)
	require.Equal(t, n, conn.WriteCount())
	for i, resp := range conn.AllResponses() {
		require.Equal(t, http.StatusAccepted, ParseGnetHTTPStatus(resp), "response %d", i+1)
	}

	AssertBudgetInvariant(t, ctx, infra.Pool, infra.Redis, stack.CampaignID)

	logChaosProof(t, "http1_pipelined_keepalive_budget", map[string]string{
		"pipelined": fmt.Sprintf("%d", n),
		"accepted":  fmt.Sprintf("%d", n),
		"budget_ok": "true",
	})
}

func truncateBytes(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

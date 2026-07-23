package ingestion

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fraudHTTP1Case documents a 2026 IVT-adjacent wire scenario and the secure expectation.
// Failures surface gaps — do not change parser behavior to make these pass without review.
type fraudHTTP1Case struct {
	id        string
	name      string
	payload   []byte
	maxBody   int64
	mustErr   bool
	wantErr   error
	mustOK    bool
	postCheck func(t *testing.T, n int, req parsedHTTPRequest, rest []byte)
}

func fraudHTTP1Cases2026() []fraudHTTP1Case {
	const maxBody = int64(1024)
	minimalPOST := []byte("POST /track HTTP/1.1\r\nContent-Length: 0\r\n\r\n")

	return []fraudHTTP1Case{
		{
			id: "H1-01", name: "cl_te_smuggling_gzip_chunked",
			payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 6\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n"),
			maxBody: maxBody, mustErr: true, wantErr: errInvalidRequest,
		},
		{
			id: "H1-02", name: "duplicate_cl_desync",
			payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 5\r\n\r\nhello"),
			maxBody: maxBody, mustErr: true, wantErr: errInvalidRequest,
		},
		{
			id: "H1-03", name: "cl_zero_smuggled_tail",
			payload: []byte("POST /track HTTP/1.1\r\nContent-Length: 0\r\n\r\nSMUGGLED"),
			maxBody: maxBody, mustOK: true,
			postCheck: func(t *testing.T, n int, req parsedHTTPRequest, rest []byte) {
				assert.Equal(t, 0, req.ContentLength)
				assert.Equal(t, n, len("POST /track HTTP/1.1\r\nContent-Length: 0\r\n\r\n"))
				assert.Equal(t, "SMUGGLED", string(rest))
			},
		},
		{
			id: "H1-04", name: "obs_fold_header_injection",
			payload: []byte("POST /track HTTP/1.1\r\nX-Forwarded-For: 1.2.3.4\r\n injected: evil\r\nContent-Length: 0\r\n\r\n"),
			maxBody: maxBody, mustErr: true, wantErr: errInvalidRequest,
		},
		{
			id: "H1-05", name: "tls_hash_oversize_header",
			payload: append(append([]byte("POST /track HTTP/1.1\r\nX-TLS-Hash: "), bytes.Repeat([]byte("a"), 10240)...), []byte("\r\nContent-Length: 0\r\n\r\n")...),
			maxBody: maxBody, mustErr: true, wantErr: errInvalidRequest,
		},
		{
			id: "H1-06", name: "sec_ch_ua_ua_mismatch_still_parses",
			payload: []byte("POST /track HTTP/1.1\r\nUser-Agent: Chrome/120\r\nSec-CH-UA: \"Safari\";v=\"17\"\r\nContent-Length: 0\r\n\r\n"),
			maxBody: maxBody, mustOK: true,
		},
		{
			id: "H1-07", name: "click_spam_pipelined_50",
			payload: bytes.Repeat(minimalPOST, 50),
			maxBody: maxBody, mustOK: true,
			postCheck: func(t *testing.T, n int, req parsedHTTPRequest, rest []byte) {
				assert.Equal(t, len(minimalPOST), n)
				assert.Equal(t, 49*len(minimalPOST), len(rest))
			},
		},
		{
			id: "H1-09", name: "x_original_method_ignored",
			payload: []byte("POST /track HTTP/1.1\r\nX-Original-Method: GET\r\nContent-Length: 0\r\n\r\n"),
			maxBody: maxBody, mustOK: true,
			postCheck: func(t *testing.T, n int, req parsedHTTPRequest, rest []byte) {
				assert.Equal(t, "POST", string(req.Method))
			},
		},
		{
			id: "H1-10", name: "http_10_downgrade",
			payload: []byte("POST /track HTTP/1.0\r\nContent-Length: 0\r\n\r\n"),
			maxBody: maxBody, mustErr: true, wantErr: errInvalidRequest,
		},
		{
			id: "H1-11", name: "homoglyph_cyrillic_track_path",
			// Cyrillic 'а' (U+0430) looks like Latin 'a' in /track
			payload: []byte("POST /tr\u0430ck HTTP/1.1\r\nContent-Length: 0\r\n\r\n"),
			maxBody: maxBody, mustErr: true, wantErr: errInvalidRequest,
		},
		{
			id: "H1-12", name: "xff_long_chain",
			payload: []byte("POST /track HTTP/1.1\r\nX-Forwarded-For: ::1, 203.0.113.1, 10.0.0.1, 192.0.2.1\r\nContent-Length: 0\r\n\r\n"),
			maxBody: maxBody, mustOK: true,
			postCheck: func(t *testing.T, n int, req parsedHTTPRequest, rest []byte) {
				assert.Contains(t, string(req.ClientIP), "203.0.113.1")
			},
		},
		{
			id: "H1-13", name: "te_chunked_with_cl_zero",
			payload: []byte("POST /track HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n"),
			maxBody: maxBody, mustErr: true, wantErr: errInvalidRequest,
		},
		{
			id: "H1-14", name: "cl_tab_prefix",
			payload: []byte("POST /track HTTP/1.1\r\nContent-Length:\t5\r\n\r\nhello"),
			maxBody: maxBody, mustOK: true,
			postCheck: func(t *testing.T, n int, req parsedHTTPRequest, rest []byte) {
				assert.Equal(t, 5, req.ContentLength)
				assert.Equal(t, "hello", string(req.Body))
			},
		},
	}
}

func TestFraudScenarios_HTTP1_2026(t *testing.T) {
	var gaps []string
	for _, tc := range fraudHTTP1Cases2026() {
		tc := tc
		t.Run(tc.id+"_"+tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					gaps = append(gaps, fmt.Sprintf("%s: PANIC %v", tc.id, r))
					t.Fatalf("panic: %v", r)
				}
			}()

			n, req, err := parseHTTP1(tc.payload, tc.maxBody)
			rest := tc.payload[n:]

			switch {
			case tc.mustErr:
				if err == nil {
					msg := fmt.Sprintf("%s [%s]: GAP expected reject (%v) got success n=%d rest=%q", tc.id, tc.name, tc.wantErr, n, truncateBytes(rest, 40))
					gaps = append(gaps, msg)
					t.Fatal(msg)
				}
				if tc.wantErr != nil && !assert.ErrorIs(t, err, tc.wantErr) {
					msg := fmt.Sprintf("%s [%s]: GAP expected %v got %v", tc.id, tc.name, tc.wantErr, err)
					gaps = append(gaps, msg)
				}
			case tc.mustOK:
				if err != nil {
					msg := fmt.Sprintf("%s [%s]: GAP expected accept got %v", tc.id, tc.name, err)
					gaps = append(gaps, msg)
					t.Fatal(msg)
				}
				require.Greater(t, n, 0)
				if tc.postCheck != nil {
					tc.postCheck(t, n, req, rest)
				}
			}
		})
	}
	if len(gaps) > 0 {
		t.Logf("fraud_http1_gaps=%d", len(gaps))
		for _, g := range gaps {
			t.Log(g)
		}
	}
	logChaosProof(t, "fraud_http1_2026", map[string]string{
		"cases": fmt.Sprintf("%d", len(fraudHTTP1Cases2026())),
		"gaps":  fmt.Sprintf("%d", len(gaps)),
	})
}

func TestFraudScenarios_HTTP1_PipelineSpam(t *testing.T) {
	const maxBody = int64(1024)
	reqLine := []byte("POST /track HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
	buf := bytes.Repeat(reqLine, 50)
	offset := 0
	for i := 0; i < 50; i++ {
		n, _, err := parseHTTP1(buf[offset:], maxBody)
		require.NoError(t, err, "pipeline iter %d", i)
		require.Equal(t, len(reqLine), n)
		offset += n
	}
	require.Equal(t, len(buf), offset)
}

func TestFraudScenarios_HTTP1_HeaderValueCRLFInjection(t *testing.T) {
	// Obs-fold style: space + continuation line in header value
	payload := []byte("POST /track HTTP/1.1\r\nX-Evil: safe\r\n continuation\r\nContent-Length: 0\r\n\r\n")
	_, _, err := parseHTTP1(payload, 1024)
	if err == nil {
		t.Fatal("GAP H1-04b: obs-fold continuation in header value accepted")
	}
}

func TestFraudScenarios_HTTP1_FraudHeaderBounds(t *testing.T) {
	const maxHeader = 1024
	h := strings.Repeat("x", maxHeader+1)
	payload := fmt.Appendf(nil, "POST /track HTTP/1.1\r\nX-TLS-Hash: %s\r\nContent-Length: 0\r\n\r\n", h)
	_, _, err := parseHTTP1(payload, 1024)
	require.ErrorIs(t, err, errInvalidRequest)
}

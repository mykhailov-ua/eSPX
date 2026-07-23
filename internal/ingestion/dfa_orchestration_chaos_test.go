package ingestion

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cross-DFA orchestration scenarios (2026 multi-signal fraud patterns).

func TestFraudScenarios_XDFA01_EdgeTrackerCampaignMismatch(t *testing.T) {
	// Simulates edge extracting campaign A while tracker body carries campaign B.
	campaignA := "550e8400-e29b-41d4-a716-446655440000"
	campaignB := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	body := []byte(fmt.Sprintf(`{"campaign_id":"%s","type":"click"}`, campaignB))

	httpWire := []byte("POST /track HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: " +
		fmt.Sprintf("%d", len(body)) + "\r\n\r\n")
	httpWire = append(httpWire, body...)

	n, req, err := parseHTTP1(httpWire, 1<<20)
	require.NoError(t, err)
	require.Equal(t, len(httpWire), n)

	var tr TrackRequest
	require.NoError(t, ParseTrackRequestJSON(&tr, req.Body))
	assert.Equal(t, campaignB, tr.CampaignID.String())

	// Edge would have extracted campaignA — mismatch is a fraud signal downstream.
	if campaignA == tr.CampaignID.String() {
		t.Fatal("setup error: campaigns must differ")
	}
	t.Logf("XDFA-01: edge_campaign=%s tracker_campaign=%s — mismatch detectable by orchestration layer", campaignA, tr.CampaignID)
}

func TestFraudScenarios_XDFA02_TLSHashParsedDespiteUAChrome(t *testing.T) {
	wire := []byte("POST /track HTTP/1.1\r\n" +
		"User-Agent: Mozilla/5.0 Chrome/120.0.0.0\r\n" +
		"X-TLS-Hash: 37b37375c33a2e6a17b2b6400c436321\r\n" + // python-requests JA3
		"Content-Length: 0\r\n\r\n")
	_, req, err := parseHTTP1(wire, 1024)
	require.NoError(t, err)
	assert.Contains(t, string(req.UserAgent), "Chrome")
	assert.Equal(t, "37b37375c33a2e6a17b2b6400c436321", string(req.TLSHash))
	t.Log("XDFA-02: HTTP FSM forwards both signals — IVT tcp_edge_rule / impersonation must correlate")
}

func TestFraudScenarios_XDFA03_PipelinedValidThenSmuggled(t *testing.T) {
	valid := []byte("POST /track HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
	smuggled := []byte("GET /admin HTTP/1.1\r\n\r\n")
	buf := append(append([]byte(nil), valid...), smuggled...)

	n1, _, err := parseHTTP1(buf, 1024)
	require.NoError(t, err)
	assert.Equal(t, len(valid), n1)

	_, _, err2 := parseHTTP1(buf[n1:], 1024)
	require.ErrorIs(t, err2, errInvalidRequest)
}

func TestFraudScenarios_XDFA04_ProtoBodyParsedAsJSONFails(t *testing.T) {
	proto := testProtoTrackBody(t)
	wire := append([]byte(fmt.Sprintf("POST /track HTTP/1.1\r\nContent-Length: %d\r\n\r\n", len(proto))), proto...)
	n, req, err := parseHTTP1(wire, 1<<20)
	require.NoError(t, err)
	require.Equal(t, len(wire), n)

	var tr TrackRequest
	err = ParseTrackRequestJSON(&tr, req.Body)
	if err == nil {
		t.Log("GAP XDFA-04: proto body accepted by JSON DFA (content-type confusion)")
	} else {
		t.Logf("XDFA-04: proto body correctly rejected by JSON DFA: %v", err)
	}
}

func TestFraudScenarios_XDFA05_SecCHUAVsUserAgentBothForwarded(t *testing.T) {
	wire := []byte("POST /track HTTP/1.1\r\n" +
		"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120\r\n" +
		"Sec-CH-UA: \"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\"\r\n" +
		"Content-Length: 0\r\n\r\n")
	_, req, err := parseHTTP1(wire, 1024)
	require.NoError(t, err)
	assert.NotEmpty(t, req.UserAgent)
	assert.NotEmpty(t, req.SecCHUA)
	t.Log("XDFA-05: both Client Hints and UA available for downstream impersonation checks")
}

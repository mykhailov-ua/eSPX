package ingestion

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/rtb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOpenRTB26ChaosHandler(t *testing.T) *AdsPacketHandler {
	t.Helper()
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)
	winnerID := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: winnerID, BudgetLimit: 50_000_000, TargetCountries: map[string]struct{}{"US": {}}}},
		map[uuid.UUID]RtbCampaignInput{
			winnerID: {BidMicro: 2_000_000, DeviceMask: 7, CategoryMask: 3, GeoHash: geo, Weight: 1},
		},
	)
	cfg := &config.Config{MaxRequestBodySize: 1 << 20}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)
	h.trackProc.rtbCatalog = catalog
	h.trackProc.rtbMode = rtbModeLive
	h.trackProc.ingestGeo = &staticGeoProvider{country: "US"}
	return h
}

func postOpenRTB26Gnet(h *AdsPacketHandler, body []byte) (int, []byte) {
	wire := BuildGnetHTTP("POST", "/openrtb/bid", map[string]string{
		"Content-Type": "application/json",
	}, body)
	_, conn := ServeGnetHarness(h, wire)
	return ParseGnetHTTPStatus(conn.Written()), conn.Written()
}

// TestChaos_OpenRTB26_TruncatedGnetCorpus verifies hostile OpenRTB 2.6 bodies through gnet never panic.
func TestChaos_OpenRTB26_TruncatedGnetCorpus(t *testing.T) {
	h := newOpenRTB26ChaosHandler(t)

	cases := []struct {
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name:       "missing_imp_array",
			body:       []byte(`{"id":"b1","tmax":300,"device":{"devicetype":2}}`),
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "truncated_id_only",
			body:       []byte(`{"id":`),
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "garbage_prefix",
			body:       []byte(`{not-json-at-all`),
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "empty_object",
			body:       []byte(`{}`),
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "valid_single_imp",
			body:       []byte(`{"id":"b1","tmax":300,"imp":[{"id":"1","bidfloor":0.5}],"device":{"devicetype":2}}`),
			wantStatus: http.StatusOK,
		},
		{
			name:       "multi_imp",
			body:       []byte(`{"id":"b2","tmax":300,"imp":[{"id":"1","bidfloor":0.5},{"id":"2","bidfloor":1.0}],"device":{"devicetype":2}}`),
			wantStatus: http.StatusOK,
		},
	}

	var nobid, bid, other int
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			status, resp := postOpenRTB26Gnet(h, tc.body)
			assert.Equal(t, tc.wantStatus, status)
			switch status {
			case http.StatusOK:
				bid++
				assert.Contains(t, string(resp), "seatbid")
			case http.StatusNoContent:
				nobid++
				assert.Contains(t, string(resp), "nbr")
			default:
				other++
			}
		})
	}

	logChaosProof(t, "openrtb26_truncated_gnet_corpus", map[string]string{
		"cases": fmt.Sprintf("%d", len(cases)),
		"bid":   fmt.Sprintf("%d", bid),
		"nobid": fmt.Sprintf("%d", nobid),
		"other": fmt.Sprintf("%d", other),
	})
}

// TestChaos_OpenRTB26_ConcurrentGnetBid hammers /openrtb/bid through gnet from many goroutines.
func TestChaos_OpenRTB26_ConcurrentGnetBid(t *testing.T) {
	h := newOpenRTB26ChaosHandler(t)

	valid := []byte(`{"id":"b1","tmax":300,"imp":[{"id":"1","bidfloor":0.5}],"device":{"devicetype":2}}`)
	truncated := []byte(`{"id":"x","imp":[{`)

	const (
		workers    = 24
		iterations = 80
	)
	var (
		panics  atomic.Uint64
		okCount atomic.Int32
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				body := valid
				if (w+i)%3 == 0 {
					body = truncated
				}
				func() {
					defer func() {
						if r := recover(); r != nil {
							panics.Add(1)
						}
					}()
					status, _ := postOpenRTB26Gnet(h, body)
					if status == http.StatusOK || status == http.StatusNoContent {
						okCount.Add(1)
					}
				}()
			}
		}()
	}
	wg.Wait()

	require.Zero(t, panics.Load(), "concurrent OpenRTB26 gnet ingress must not panic")
	require.Equal(t, int32(workers*iterations), okCount.Load())

	logChaosProof(t, "openrtb26_concurrent_gnet_bid", map[string]string{
		"workers":    fmt.Sprintf("%d", workers),
		"iterations": fmt.Sprintf("%d", iterations),
		"responses":  fmt.Sprintf("%d", okCount.Load()),
		"panics":     fmt.Sprintf("%d", panics.Load()),
	})
}

package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/edge/xdpstats"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOperatorDashboard_XDPPanel(t *testing.T) {
	h := &DashboardsHTTPHandlers{
		XDPStatsReader: func(context.Context) (xdpstats.Snapshot, error) {
			return xdpstats.Snapshot{
				UpdatedAt: time.Unix(1_700_000_000, 0).UTC(),
				Pass:      1000,
				Drops: map[string]uint64{
					"syn":     42,
					"anomaly": 7,
				},
				Fingerprints: 128,
			}, nil
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboards/operator", nil)
	rec := httptest.NewRecorder()
	h.getOperatorDashboard(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp OperatorDashboardDTO
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, uint64(1000), resp.XDP.Pass)
	assert.Equal(t, uint64(42), resp.XDP.Drops["syn"])
	assert.Equal(t, uint64(128), resp.XDP.Fingerprints)
}

package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReports_FreshnessWithoutCH(t *testing.T) {
	t.Parallel()

	h := &ReportsHTTPHandlers{}
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/api/v1/reports/placements?limit=1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp PlacementReportResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.True(t, resp.Freshness.Stale)
	assert.Equal(t, 0, resp.Freshness.CHLagSeconds)
}

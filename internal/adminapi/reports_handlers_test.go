package adminapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteRegistration(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	reportsHandler := &ReportsHTTPHandlers{}
	dashboardsHandler := &DashboardsHTTPHandlers{}
	viewsHandler := &ViewsHTTPHandlers{Service: NewService()}

	registry := RouteRegistry{
		ReportsHTTP:    reportsHandler,
		DashboardsHTTP: dashboardsHandler,
		ViewsHTTP:      viewsHandler,
	}

	RegisterRoutes(mux, registry)

	// Verify routes are registered by sending requests and checking they don't return 404
	// (they might return 403 or 200, but not 404)
	routes := []struct {
		method string
		path   string
	}{
		{"GET", "/api/v1/reports/placements"},
		{"GET", "/api/v1/reports/keywords"},
		{"GET", "/api/v1/dashboards/campaign/" + uuid.New().String()},
		{"GET", "/api/v1/views?customer_id=" + uuid.New().String()},
		{"POST", "/api/v1/views"},
	}

	for _, rt := range routes {
		req := httptest.NewRequest(rt.method, rt.path, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.NotEqual(t, http.StatusNotFound, w.Code, "route %s %s not registered", rt.method, rt.path)
	}
}

func TestReports_Placements(t *testing.T) {
	t.Parallel()

	h := &ReportsHTTPHandlers{}
	mux := http.NewServeMux()
	h.Register(mux)

	// 1. Test basic request
	req := httptest.NewRequest("GET", "/api/v1/reports/placements?limit=5", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp PlacementReportResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Len(t, resp.Rows, 5)
	assert.True(t, resp.Freshness.Stale)
	assert.Equal(t, 0, resp.Freshness.CHLagSeconds)
	assert.NotEmpty(t, resp.NextCursor)

	// 2. Test pagination with cursor
	req2 := httptest.NewRequest("GET", "/api/v1/reports/placements?limit=5&cursor="+resp.NextCursor, nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	require.Equal(t, http.StatusOK, w2.Code)

	var resp2 PlacementReportResponse
	err = json.Unmarshal(w2.Body.Bytes(), &resp2)
	require.NoError(t, err)

	assert.Len(t, resp2.Rows, 5)
	assert.NotEqual(t, resp.Rows[0].PlacementID, resp2.Rows[0].PlacementID)
}

func TestReports_Keywords(t *testing.T) {
	t.Parallel()

	h := &ReportsHTTPHandlers{}
	mux := http.NewServeMux()
	h.Register(mux)

	// 1. Test basic request
	req := httptest.NewRequest("GET", "/api/v1/reports/keywords?limit=5", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp KeywordReportResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Len(t, resp.Rows, 5)
	assert.True(t, resp.Freshness.Stale)
	assert.Equal(t, 0, resp.Freshness.CHLagSeconds)
	assert.NotEmpty(t, resp.NextCursor)

	// 2. Test pagination with cursor
	req2 := httptest.NewRequest("GET", "/api/v1/reports/keywords?limit=5&cursor="+resp.NextCursor, nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	require.Equal(t, http.StatusOK, w2.Code)

	var resp2 KeywordReportResponse
	err = json.Unmarshal(w2.Body.Bytes(), &resp2)
	require.NoError(t, err)

	assert.Len(t, resp2.Rows, 5)
	assert.NotEqual(t, resp.Rows[0].Keyword, resp2.Rows[0].Keyword)
}

func TestDashboards_Campaign(t *testing.T) {
	t.Parallel()

	h := &DashboardsHTTPHandlers{}
	mux := http.NewServeMux()
	h.Register(mux)

	campaignID := uuid.New()
	req := httptest.NewRequest("GET", "/api/v1/dashboards/campaign/"+campaignID.String(), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp CampaignDashboardDTO
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, campaignID.String(), resp.CampaignID)
	assert.Equal(t, int64(150000000), resp.KPIs.SpendMicro)
	assert.Equal(t, int64(180000000), resp.KPIs.RevenueMicro)
	assert.True(t, resp.Freshness.Stale)
}

func TestViews_CRUD(t *testing.T) {
	t.Parallel()

	h := &ViewsHTTPHandlers{Service: NewService()}
	mux := http.NewServeMux()
	h.Register(mux)

	customerID := uuid.New().String()

	// 1. Create View
	createReq := CreateViewRequest{
		CustomerID: customerID,
		Name:       "My Placement View",
		ReportKey:  "placements",
		Spec:       map[string]any{"limit": 10},
		IsShared:   true,
	}
	body, _ := json.Marshal(createReq)
	req := httptest.NewRequest("POST", "/api/v1/views", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusCreated, w.Code)

	var created SavedViewDTO
	err := json.Unmarshal(w.Body.Bytes(), &created)
	require.NoError(t, err)

	assert.NotEmpty(t, created.ID)
	assert.Equal(t, createReq.Name, created.Name)
	assert.Equal(t, createReq.CustomerID, created.CustomerID)

	// 2. List Views
	reqList := httptest.NewRequest("GET", "/api/v1/views?customer_id="+customerID, nil)
	wList := httptest.NewRecorder()
	mux.ServeHTTP(wList, reqList)

	require.Equal(t, http.StatusOK, wList.Code)

	var list []SavedViewDTO
	err = json.Unmarshal(wList.Body.Bytes(), &list)
	require.NoError(t, err)

	assert.Len(t, list, 1)
	assert.Equal(t, created.ID, list[0].ID)

	// 3. Get View
	reqGet := httptest.NewRequest("GET", "/api/v1/views/"+created.ID, nil)
	wGet := httptest.NewRecorder()
	mux.ServeHTTP(wGet, reqGet)

	require.Equal(t, http.StatusOK, wGet.Code)

	var fetched SavedViewDTO
	err = json.Unmarshal(wGet.Body.Bytes(), &fetched)
	require.NoError(t, err)

	assert.Equal(t, created.ID, fetched.ID)

	// 4. Update View
	updateReq := UpdateViewRequest{
		Name:      "Updated View Name",
		ReportKey: "placements",
		Spec:      map[string]any{"limit": 20},
		IsShared:  false,
	}
	updateBody, _ := json.Marshal(updateReq)
	reqUpdate := httptest.NewRequest("PUT", "/api/v1/views/"+created.ID, bytes.NewReader(updateBody))
	wUpdate := httptest.NewRecorder()
	mux.ServeHTTP(wUpdate, reqUpdate)

	require.Equal(t, http.StatusOK, wUpdate.Code)

	var updated SavedViewDTO
	err = json.Unmarshal(wUpdate.Body.Bytes(), &updated)
	require.NoError(t, err)

	assert.Equal(t, updateReq.Name, updated.Name)
	assert.False(t, updated.IsShared)

	// 5. Delete View
	reqDelete := httptest.NewRequest("DELETE", "/api/v1/views/"+created.ID, nil)
	wDelete := httptest.NewRecorder()
	mux.ServeHTTP(wDelete, reqDelete)

	require.Equal(t, http.StatusNoContent, wDelete.Code)

	// Verify deleted
	reqGet2 := httptest.NewRequest("GET", "/api/v1/views/"+created.ID, nil)
	wGet2 := httptest.NewRecorder()
	mux.ServeHTTP(wGet2, reqGet2)

	assert.Equal(t, http.StatusNotFound, wGet2.Code)
}

func TestToPlacementReportRowDTO(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		row  placementReportCHRow
		want PlacementReportRowDTO
	}{
		{
			name: "profit and roi",
			row: placementReportCHRow{
				PlacementID:  "zone_1001",
				CampaignID:   "camp-1",
				Impressions:  10000,
				Clicks:       500,
				Conversions:  10,
				SpendMicro:   50_000_000,
				RevenueMicro: 60_000_000,
			},
			want: PlacementReportRowDTO{
				PlacementID:  "zone_1001",
				CampaignID:   "camp-1",
				Impressions:  10000,
				Clicks:       500,
				Conversions:  10,
				SpendMicro:   50_000_000,
				RevenueMicro: 60_000_000,
				ProfitMicro:  10_000_000,
				ROIPct:       20,
				CPAMicro:     5_000_000,
			},
		},
		{
			name: "zero spend skips roi",
			row: placementReportCHRow{
				PlacementID: "zone_0",
				CampaignID:  "camp-2",
			},
			want: PlacementReportRowDTO{
				PlacementID: "zone_0",
				CampaignID:  "camp-2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toPlacementReportRowDTO(tt.row)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToKeywordReportRowDTO(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		row  keywordReportCHRow
		want KeywordReportRowDTO
	}{
		{
			name: "profit and roi",
			row: keywordReportCHRow{
				Keyword:      "insurance",
				CampaignID:   "camp-1",
				Impressions:  5000,
				Clicks:       200,
				Conversions:  5,
				SpendMicro:   25_000_000,
				RevenueMicro: 30_000_000,
			},
			want: KeywordReportRowDTO{
				Keyword:      "insurance",
				CampaignID:   "camp-1",
				Impressions:  5000,
				Clicks:       200,
				Conversions:  5,
				SpendMicro:   25_000_000,
				RevenueMicro: 30_000_000,
				ProfitMicro:  5_000_000,
				ROIPct:       20,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toKeywordReportRowDTO(tt.row)
			assert.Equal(t, tt.want, got)
		})
	}
}

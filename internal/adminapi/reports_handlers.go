package adminapi

import (
	"errors"
	"espx/pkg/httpresponse"
	"net/http"

	"github.com/google/uuid"
)

// ReportsHTTPHandlers serves tabular report JSON routes (M4 facet; queries land in M6 CHG waves).
type ReportsHTTPHandlers struct {
	CampaignStats             CampaignStatsReader
	CampaignForecaster        CampaignForecaster
	ApplyRateLimit            func(http.HandlerFunc) http.HandlerFunc
	RequirePermission         func(string, http.HandlerFunc) http.HandlerFunc
	AuthorizeCampaignAccess   func(*http.Request, uuid.UUID) error
	ResolveForecastCustomerID func(*http.Request, *uuid.UUID) (*uuid.UUID, error)
	WriteServiceError         func(http.ResponseWriter, error)
}

// Register mounts report routes on mux.
func (h *ReportsHTTPHandlers) Register(mux *http.ServeMux) {
	if h == nil {
		return
	}
	if h.ApplyRateLimit == nil {
		h.ApplyRateLimit = func(next http.HandlerFunc) http.HandlerFunc { return next }
	}
	if h.RequirePermission == nil {
		h.RequirePermission = func(_ string, next http.HandlerFunc) http.HandlerFunc { return next }
	}

	h.registerCampaignStats(mux)
	h.registerCampaignForecast(mux)
	h.registerScaffoldReports(mux)
}

func (h *ReportsHTTPHandlers) registerScaffoldReports(mux *http.ServeMux) {
	limit := h.ApplyRateLimit
	perm := h.RequirePermission

	routes := []struct {
		path       string
		permission string
	}{
		{"GET /api/v1/reports/campaign-unit-economics", "campaigns:read"},
		{"GET /api/v1/reports/source-margin", "campaigns:read"},
		{"GET /api/v1/reports/traffic-sources", "campaigns:read"},
		{"GET /api/v1/reports/source-quality", "campaigns:read"},
		{"GET /api/v1/reports/spend-velocity", "campaigns:read"},
		{"GET /api/v1/reports/campaign-geo-device", "campaigns:read"},
		{"GET /api/v1/reports/geo-roi", "campaigns:read"},
		{"GET /api/v1/reports/daypart-heatmap", "campaigns:read"},
		{"GET /api/v1/reports/pacing-drift", "campaigns:read"},
		{"GET /api/v1/reports/postback-reconciliation", "customers:read"},
		{"GET /api/v1/reports/ivt-by-source", "audit:read"},
		{"GET /api/v1/reports/discrepancy-buy-sell", "customers:read"},
		{"GET /api/v1/reports/campaign-overview", "campaigns:read"},
		{"GET /api/v1/reports/customer-portfolio", "customers:read"},
	}
	for _, route := range routes {
		mux.HandleFunc(route.path, limit(perm(route.permission, h.notImplemented)))
	}
	mux.HandleFunc("POST /api/v1/reports/jobs", limit(perm("customers:read", h.notImplemented)))
}

func (h *ReportsHTTPHandlers) notImplemented(w http.ResponseWriter, _ *http.Request) {
	httpresponse.Error(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "reports facet scaffold; see MILESTONE.md M6")
}

func (h *ReportsHTTPHandlers) writeServiceError(w http.ResponseWriter, err error) {
	var q invalidQueryError
	if errors.As(err, &q) {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", string(q))
		return
	}
	if errors.Is(err, ErrForbidden) {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	if h.WriteServiceError != nil {
		h.WriteServiceError(w, err)
		return
	}
	httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
}

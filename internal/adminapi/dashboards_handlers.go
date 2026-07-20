package adminapi

import (
	"net/http"
	"time"

	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

// DashboardsHTTPHandlers serves persona dashboard JSON routes (M6 waves).
type DashboardsHTTPHandlers struct {
	ApplyRateLimit    func(http.HandlerFunc) http.HandlerFunc
	RequirePermission func(string, http.HandlerFunc) http.HandlerFunc
}

// Register mounts dashboard routes on mux.
func (h *DashboardsHTTPHandlers) Register(mux *http.ServeMux) {
	if h == nil {
		return
	}
	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	if limit == nil {
		limit = func(next http.HandlerFunc) http.HandlerFunc { return next }
	}
	if perm == nil {
		perm = func(_ string, next http.HandlerFunc) http.HandlerFunc { return next }
	}

	mux.HandleFunc("GET /api/v1/dashboards/buyer", limit(perm("campaigns:read", h.notImplemented)))
	mux.HandleFunc("GET /api/v1/dashboards/adops", limit(perm("campaigns:read", h.notImplemented)))
	mux.HandleFunc("GET /api/v1/dashboards/accountant", limit(perm("customers:read", h.notImplemented)))
	mux.HandleFunc("GET /api/v1/dashboards/cfo", limit(perm("customers:read", h.notImplemented)))
	mux.HandleFunc("GET /api/v1/dashboards/fraud", limit(perm("audit:read", h.notImplemented)))
	mux.HandleFunc("GET /api/v1/dashboards/operator", limit(perm("shards:read", h.notImplemented)))
	mux.HandleFunc("GET /api/v1/dashboards/campaign/{id}", limit(perm("campaigns:read", h.getCampaignDashboard)))
}

func (h *DashboardsHTTPHandlers) getCampaignDashboard(w http.ResponseWriter, r *http.Request) {
	campaignIDStr := r.PathValue("id")
	campaignID, err := uuid.Parse(campaignIDStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}

	resp := CampaignDashboardDTO{
		CampaignID: campaignID.String(),
		KPIs: MetricsBlockDTO{
			SpendMicro:   150000000,
			RevenueMicro: 180000000,
			ProfitMicro:  30000000,
			Conversions:  120,
			CPAMicro:     1250000,
			ROIPct:       20.0,
			Freshness: DataFreshnessDTO{
				AsOf:         time.Now().UTC().Format(time.RFC3339),
				Consistency:  "eventual",
				Stale:        true,
				CHLagSeconds: 360,
			},
		},
		Freshness: DataFreshnessDTO{
			AsOf:         time.Now().UTC().Format(time.RFC3339),
			Consistency:  "eventual",
			Stale:        true,
			CHLagSeconds: 360,
		},
	}

	httpresponse.JSON(w, http.StatusOK, resp)
}

func (h *DashboardsHTTPHandlers) notImplemented(w http.ResponseWriter, _ *http.Request) {
	httpresponse.Error(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "dashboard scaffold; see MILESTONE.md M6")
}

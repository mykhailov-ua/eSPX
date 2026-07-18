package adminapi

import (
	"net/http"

	"espx/pkg/httpresponse"
)

// DashboardsHTTPHandlers serves persona dashboard JSON routes (M4 facet; implementation lands in M6 waves).
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
}

func (h *DashboardsHTTPHandlers) notImplemented(w http.ResponseWriter, _ *http.Request) {
	httpresponse.Error(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "dashboard facet scaffold; see MILESTONE.md M6")
}

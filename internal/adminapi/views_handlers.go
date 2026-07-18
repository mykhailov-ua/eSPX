package adminapi

import (
	"net/http"

	"espx/pkg/httpresponse"
)

// ViewsHTTPHandlers serves saved report view CRUD (M4 facet; persistence in M6 ADM-W5).
type ViewsHTTPHandlers struct {
	Service           *Service
	ApplyRateLimit    func(http.HandlerFunc) http.HandlerFunc
	RequirePermission func(string, http.HandlerFunc) http.HandlerFunc
}

// Register mounts saved view routes on mux.
func (h *ViewsHTTPHandlers) Register(mux *http.ServeMux) {
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

	mux.HandleFunc("GET /api/v1/views", limit(perm("campaigns:read", h.notImplemented)))
	mux.HandleFunc("POST /api/v1/views", limit(perm("campaigns:write", h.notImplemented)))
	mux.HandleFunc("GET /api/v1/views/{id}", limit(perm("campaigns:read", h.notImplemented)))
	mux.HandleFunc("PUT /api/v1/views/{id}", limit(perm("campaigns:write", h.notImplemented)))
	mux.HandleFunc("DELETE /api/v1/views/{id}", limit(perm("campaigns:write", h.notImplemented)))
}

func (h *ViewsHTTPHandlers) notImplemented(w http.ResponseWriter, _ *http.Request) {
	httpresponse.Error(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "views facet scaffold; see MILESTONE.md M6")
}

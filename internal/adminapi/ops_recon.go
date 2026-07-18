package adminapi

import (
	"net/http"

	"espx/pkg/coldpath"
)

func (h *OpsHTTPHandlers) registerReconRoutes(mux *http.ServeMux) {
	if h.OpsReader == nil {
		return
	}
	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	mux.HandleFunc("GET /api/v1/recon/runs", limit(perm("audit:read", h.listReconRuns)))
}

func (h *OpsHTTPHandlers) listReconRuns(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	limit, offset := parseAPIPagination(r)

	runs, total, err := h.OpsReader.ListReconRuns(r.Context(), service, limit, offset)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	coldpath.WritePaginatedJSON(w, runs, total)
}

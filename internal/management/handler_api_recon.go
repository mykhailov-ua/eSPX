package management

import (
	"net/http"

	"espx/pkg/coldpath"
)

func (h *Handler) listReconRuns(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	limit, offset := parseAPIPagination(r)

	runs, total, err := h.svc.ListReconRuns(r.Context(), service, limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	coldpath.WritePaginatedJSON(w, runs, total)
}

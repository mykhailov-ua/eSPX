package management

import (
	"net/http"

	"espx/pkg/httpresponse"
)

func (h *Handler) registerOpsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/ops/shards", h.limit(h.perm(h.getShardHealth, PermShardsRead)))
}

func (h *Handler) getShardHealth(w http.ResponseWriter, r *http.Request) {
	report, err := h.svc.GetShardHealth(r.Context())
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusOK, report)
}

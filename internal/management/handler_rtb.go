package management

import (
	"net/http"
	"time"

	"espx/internal/config"
	"espx/internal/ingestion"
	"espx/pkg/coldpath"
	"espx/pkg/httpresponse"
)

// registerRtbRoutes mounts OpenRTB control plane admin endpoints.
func (h *Handler) registerRtbRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/rtb/deals", h.limit(h.perm(h.listRtbDeals, PermSettingsRead)))
	mux.HandleFunc("POST /admin/rtb/deals", h.limit(h.perm(h.createRtbDeal, PermSettingsWrite)))
	mux.HandleFunc("GET /admin/rtb/deals/{id}", h.limit(h.perm(h.getRtbDeal, PermSettingsRead)))
	mux.HandleFunc("PUT /admin/rtb/deals/{id}", h.limit(h.perm(h.updateRtbDeal, PermSettingsWrite)))
	mux.HandleFunc("DELETE /admin/rtb/deals/{id}", h.limit(h.perm(h.deleteRtbDeal, PermSettingsWrite)))
	mux.HandleFunc("POST /admin/rtb/validate-bid-request", h.limit(h.perm(h.validateBidRequest, PermSettingsRead)))
	mux.HandleFunc("POST /admin/rtb/bid-shade", h.limit(h.perm(h.postRtbBidShade, PermSettingsRead)))
	mux.HandleFunc("GET /admin/rtb/shadow-diff", h.limit(h.perm(h.getRtbShadowDiff, PermSettingsRead)))
	mux.HandleFunc("GET /admin/rtb/live-gate", h.limit(h.perm(h.getRtbLiveGate, PermSettingsRead)))
	mux.HandleFunc("POST /admin/rtb/mode", h.limit(h.perm(h.setRtbMode, PermSettingsWrite)))
}

func (h *Handler) listRtbDeals(w http.ResponseWriter, r *http.Request) {
	rows, err := h.svc.ListRtbDeals(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]any{"deals": rows})
}

func (h *Handler) getRtbDeal(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid deal id")
		return
	}
	row, err := h.svc.GetRtbDeal(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, row)
}

func (h *Handler) createRtbDeal(w http.ResponseWriter, r *http.Request) {
	spec, err := coldpath.DecodeRequest[RtbDealCreateSpec](w, r, coldpath.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	row, err := h.svc.CreateRtbDeal(r.Context(), spec)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusCreated, row)
}

func (h *Handler) updateRtbDeal(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid deal id")
		return
	}
	spec, err := coldpath.DecodeRequest[RtbDealUpdateSpec](w, r, coldpath.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	row, err := h.svc.UpdateRtbDeal(r.Context(), id, spec)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, row)
}

func (h *Handler) deleteRtbDeal(w http.ResponseWriter, r *http.Request) {
	id, err := parsePathInt64(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid deal id")
		return
	}
	if err := h.svc.DeleteRtbDeal(r.Context(), id); err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) validateBidRequest(w http.ResponseWriter, r *http.Request) {
	body, err := coldpath.ReadLimitedBody(w, r, coldpath.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	result := ingestion.ValidateOpenRTBBidRequest(body)
	httpresponse.JSON(w, http.StatusOK, result)
}

func (h *Handler) postRtbBidShade(w http.ResponseWriter, r *http.Request) {
	req, err := coldpath.DecodeRequest[RtbBidShadeRequest](w, r, coldpath.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	resp, err := h.svc.SimulateRtbBidShade(r.Context(), req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, resp)
}

func (h *Handler) getRtbShadowDiff(w http.ResponseWriter, r *http.Request) {
	window := parseDurationQuery(r, "window", time.Hour)
	snap := ingestion.RtbShadowDiffForWindow(window)
	httpresponse.JSON(w, http.StatusOK, snap)
}

func (h *Handler) getRtbLiveGate(w http.ResponseWriter, r *http.Request) {
	window := parseDurationQuery(r, "window", time.Hour)
	gate := ingestion.EvaluateRtbLiveGate(window)
	httpresponse.JSON(w, http.StatusOK, gate)
}

type rtbModeRequest struct {
	Mode string `json:"mode"`
}

func (h *Handler) setRtbMode(w http.ResponseWriter, r *http.Request) {
	req, err := coldpath.DecodeRequest[rtbModeRequest](w, r, coldpath.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	mode := config.ParseRtbMode(req.Mode)
	if mode == config.RtbModeLive {
		ready, reasons := ingestion.CanEnableRtbLive(time.Hour)
		if !ready {
			httpresponse.JSON(w, http.StatusConflict, map[string]any{
				"error":   "unsafe_live_cutover",
				"reasons": reasons,
			})
			return
		}
	}
	if err := h.svc.SetRtbMode(r.Context(), req.Mode); err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"mode": req.Mode})
}

func parseDurationQuery(r *http.Request, key string, fallback time.Duration) time.Duration {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

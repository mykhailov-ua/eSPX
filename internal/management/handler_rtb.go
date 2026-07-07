package management

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"espx/internal/ads"
	"espx/pkg/cold"
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
	mux.HandleFunc("GET /admin/rtb/shadow-diff", h.limit(h.perm(h.getRtbShadowDiff, PermSettingsRead)))
}

func (h *Handler) listRtbDeals(w http.ResponseWriter, r *http.Request) {
	rows, err := h.svc.ListRtbDeals(r.Context())
	if err != nil {
		writeRtbDealError(w, err)
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
		writeRtbDealError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, row)
}

func (h *Handler) createRtbDeal(w http.ResponseWriter, r *http.Request) {
	spec, err := cold.DecodeRequest[RtbDealCreateSpec](w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	row, err := h.svc.CreateRtbDeal(r.Context(), spec)
	if err != nil {
		writeRtbDealError(w, err)
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
	spec, err := cold.DecodeRequest[RtbDealUpdateSpec](w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	row, err := h.svc.UpdateRtbDeal(r.Context(), id, spec)
	if err != nil {
		writeRtbDealError(w, err)
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
		writeRtbDealError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) validateBidRequest(w http.ResponseWriter, r *http.Request) {
	body, err := cold.ReadLimitedBody(w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	result := ads.ValidateOpenRTBBidRequest(body)
	httpresponse.JSON(w, http.StatusOK, result)
}

func (h *Handler) getRtbShadowDiff(w http.ResponseWriter, r *http.Request) {
	window := parseDurationQuery(r, "window", time.Hour)
	snap := ads.RtbShadowDiffForWindow(window)
	httpresponse.JSON(w, http.StatusOK, snap)
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

func writeRtbDealError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRtbDealNotFound), errors.Is(err, ErrDealCustomerMissing):
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", err.Error())
	case errors.Is(err, ErrInvalidDealPacing), errors.Is(err, ErrDuplicateDealID), errors.Is(err, ErrInvalidDealSeats):
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
	default:
		msg := err.Error()
		if strings.Contains(msg, "required") || strings.Contains(msg, "must be") {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", msg)
			return
		}
		writeServiceError(w, err)
	}
}

package adminapi

import (
	"encoding/json"
	"net/http"

	"espx/internal/management"
	"espx/internal/marginguard"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

type MarginGuardHTTPHandlers struct {
	svc *management.Service
}

func NewMarginGuardHTTPHandlers(svc *management.Service) *MarginGuardHTTPHandlers {
	return &MarginGuardHTTPHandlers{svc: svc}
}

func (h *MarginGuardHTTPHandlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/margin-guard/policies", h.listPolicies)
	mux.HandleFunc("POST /api/v1/margin-guard/policies", h.createPolicy)
	mux.HandleFunc("GET /api/v1/margin-guard/activity", h.listActivity)
	mux.HandleFunc("POST /api/v1/margin-guard/overrides", h.removeOverride)
}

func (h *MarginGuardHTTPHandlers) listPolicies(w http.ResponseWriter, r *http.Request) {
	campIDStr := r.URL.Query().Get("campaign_id")
	campID, err := uuid.Parse(campIDStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign_id")
		return
	}

	policies, err := h.svc.ListMarginGuardPolicies(r.Context(), campID)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusOK, policies)
}

func (h *MarginGuardHTTPHandlers) createPolicy(w http.ResponseWriter, r *http.Request) {
	var p marginguard.Policy
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}

	if err := h.svc.CreateMarginGuardPolicy(r.Context(), &p); err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusCreated, p)
}

func (h *MarginGuardHTTPHandlers) listActivity(w http.ResponseWriter, r *http.Request) {
	campIDStr := r.URL.Query().Get("campaign_id")
	campID, err := uuid.Parse(campIDStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign_id")
		return
	}

	activity, err := h.svc.GetMarginGuardActivity(r.Context(), campID)
	if err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusOK, activity)
}

func (h *MarginGuardHTTPHandlers) removeOverride(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CampaignID  string `json:"campaign_id"`
		PlacementID string `json:"placement_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}

	campID, err := uuid.Parse(req.CampaignID)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign_id")
		return
	}

	if err := h.svc.RemovePlacementOverride(r.Context(), campID, req.PlacementID); err != nil {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

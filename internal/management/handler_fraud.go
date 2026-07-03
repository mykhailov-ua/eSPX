package management

import (
	"net/http"

	"espx/pkg/cold"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

// registerFraudRoutes mounts campaign fraud configuration endpoints.
func (h *Handler) registerFraudRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/campaigns/{id}/fraud-config", h.limit(h.perm(h.getCampaignFraudConfig, PermCampaignsRead)))
	mux.HandleFunc("POST /admin/campaigns/{id}/fraud-config", h.limit(h.perm(h.updateCampaignFraudConfig, PermCampaignsWrite)))
}

func (h *Handler) getCampaignFraudConfig(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	cfg, err := h.svc.GetCampaignFraudConfig(r.Context(), campaignID)
	if err != nil {
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", "campaign not found")
		return
	}
	httpresponse.JSON(w, http.StatusOK, cfg)
}

func (h *Handler) updateCampaignFraudConfig(w http.ResponseWriter, r *http.Request) {
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	req, err := cold.DecodeRequest[CampaignFraudConfigUpdate](w, r, 4096)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	cfg, err := h.svc.UpdateCampaignFraudConfig(r.Context(), campaignID, req)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusOK, cfg)
}

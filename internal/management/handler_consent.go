package management

import (
	"encoding/json"
	"io"
	"net/http"

	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

// postConsent handles POST /api/v1/consent with HMAC-SHA256 body auth (M6.2).
func (h *Handler) postConsent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid body")
		return
	}
	sig := r.Header.Get("X-Consent-Signature")
	if err := VerifyConsentHMAC([]byte(h.cfg.ConsentHMACSecret), body, sig); err != nil {
		httpresponse.Error(w, http.StatusUnauthorized, "INVALID_SIGNATURE", "consent signature invalid")
		return
	}
	var in ConsentRecordInput
	if err := json.Unmarshal(body, &in); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
		return
	}
	if err := h.svc.RecordConsent(r.Context(), in); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// postCampaignConsentRequirements handles POST /admin/campaigns/{id}/consent-requirements (M6.3).
func (h *Handler) postCampaignConsentRequirements(w http.ResponseWriter, r *http.Request) {
	campaignID, err := parsePathUUID(r, "id")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid campaign id")
		return
	}
	var req struct {
		RequireConsentPurposes int16 `json:"require_consent_purposes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
		return
	}
	if err := h.svc.UpdateCampaignConsentRequirements(r.Context(), campaignID, req.RequireConsentPurposes); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// postPrivacyErasure handles POST /admin/privacy/erasure (M6.4).
func (h *Handler) postPrivacyErasure(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "user_id required")
		return
	}
	id, err := h.svc.CreatePrivacyErasureRequest(r.Context(), req.UserID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusAccepted, map[string]string{"request_id": id.String()})
}

func parsePathUUID(r *http.Request, key string) (uuid.UUID, error) {
	return uuid.Parse(r.PathValue(key))
}

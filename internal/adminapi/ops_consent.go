package adminapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"espx/pkg/httpresponse"
)

// ConsentRecord is the signed body for POST /api/v1/consent.
type ConsentRecord struct {
	UserID    string `json:"user_id"`
	Purposes  int16  `json:"purposes"`
	Source    string `json:"source"`
	Timestamp string `json:"timestamp,omitempty"`
}

// ConsentRecorder persists verified consent payloads.
type ConsentRecorder interface {
	RecordConsent(ctx context.Context, in ConsentRecord) error
}

// ConsentVerifier validates HMAC signatures on consent webhook bodies.
type ConsentVerifier interface {
	Verify(body []byte, signature string) error
}

func (h *OpsHTTPHandlers) registerConsentRoutes(mux *http.ServeMux) {
	if h.ConsentRecorder == nil || h.ConsentVerifier == nil {
		return
	}
	limit := h.ApplyRateLimit
	mux.HandleFunc("POST /api/v1/consent", limit(h.postConsent))
}

func (h *OpsHTTPHandlers) postConsent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid body")
		return
	}
	sig := r.Header.Get("X-Consent-Signature")
	if err := h.ConsentVerifier.Verify(body, sig); err != nil {
		httpresponse.Error(w, http.StatusUnauthorized, "INVALID_SIGNATURE", "consent signature invalid")
		return
	}
	var in ConsentRecord
	if err := json.Unmarshal(body, &in); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
		return
	}
	if err := h.ConsentRecorder.RecordConsent(r.Context(), in); err != nil {
		h.writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

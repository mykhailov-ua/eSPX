package management

import (
	"net/http"

	"espx/pkg/httpresponse"
)

// registerNotificationRoutes mounts operator notification retry endpoints.
func registerNotificationRoutes(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("POST /admin/notifications/{id}/retry", h.limit(h.perm(h.retryNotification, PermSettingsWrite)))
}

// retryNotification handles POST /admin/notifications/{id}/retry.
func (h *Handler) retryNotification(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "notification id required")
		return
	}
	if err := h.svc.RetryNotification(r.Context(), id); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "RETRY_FAILED", err.Error())
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "PENDING"})
}

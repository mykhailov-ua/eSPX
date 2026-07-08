package management

import (
	"net/http"

	"espx/pkg/cold"
	"espx/pkg/httpresponse"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type replayWebhookRequest struct {
	Provider        string `json:"provider"`
	ProviderEventID string `json:"provider_event_id"`
}

// replayPaymentWebhook handles POST /admin/payment/webhooks/replay for ops recovery.
func (h *Handler) replayPaymentWebhook(w http.ResponseWriter, r *http.Request) {
	if h.payment == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "PAYMENT_UNAVAILABLE", "payment service not configured")
		return
	}

	req, err := cold.DecodeRequest[replayWebhookRequest](w, r, 16*1024)
	if err != nil {
		return
	}
	if req.Provider == "" || req.ProviderEventID == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "provider and provider_event_id are required")
		return
	}

	resp, err := h.payment.ReplayWebhook(r.Context(), req.Provider, req.ProviderEventID)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", st.Message())
				return
			case codes.NotFound:
				httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", st.Message())
				return
			}
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "webhook replay failed")
		return
	}

	httpresponse.JSON(w, http.StatusOK, map[string]any{"status": resp.Status})
}

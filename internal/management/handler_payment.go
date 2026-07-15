package management

import (
	"net/http"

	"espx/pkg/coldpath"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// createPaymentIntentRequest carries the amount forwarded to payment gRPC after admin RBAC checks pass.
type createPaymentIntentRequest struct {
	AmountMicro int64  `json:"amount_micro"`
	Currency    string `json:"currency"`
}

// createCustomerPaymentIntent proxies admin top-ups to payment gRPC after RBAC on the management handler.
func (h *Handler) createCustomerPaymentIntent(w http.ResponseWriter, r *http.Request) {
	if h.payment == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "PAYMENT_UNAVAILABLE", "payment service not configured")
		return
	}

	customerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	body, err := coldpath.ReadLimitedBody(w, r, 16*1024)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}

	var req createPaymentIntentRequest
	if len(body) > 0 {
		req, err = coldpath.DecodeBody[createPaymentIntentRequest](body)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
			return
		}
	}
	if req.AmountMicro <= 0 {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "amount_micro must be greater than zero")
		return
	}
	currency := req.Currency
	if currency == "" {
		currency = "USD"
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "Idempotency-Key header is required")
		return
	}

	resp, err := h.payment.CreatePaymentIntent(r.Context(), customerID.String(), req.AmountMicro, currency, idempotencyKey, nil)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", st.Message())
				return
			case codes.AlreadyExists:
				httpresponse.Error(w, http.StatusConflict, "CONFLICT", st.Message())
				return
			case codes.FailedPrecondition:
				httpresponse.Error(w, http.StatusServiceUnavailable, "PAYMENT_UNAVAILABLE", st.Message())
				return
			}
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "failed to create payment intent")
		return
	}

	httpresponse.JSON(w, http.StatusOK, map[string]any{
		"intent_id":    resp.IntentId,
		"status":       resp.Status.String(),
		"checkout_url": resp.CheckoutUrl,
		"provider_ref": resp.ProviderRef,
	})
}

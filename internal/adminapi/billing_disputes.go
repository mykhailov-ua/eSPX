package adminapi

import (
	"context"
	"net/http"
	"strconv"

	"espx/pkg/httpresponse"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DisputeRowDTO is one disputed payment intent with optional chargeback ledger IDs.
type DisputeRowDTO struct {
	IntentID                 string  `json:"intent_id"`
	CustomerID               string  `json:"customer_id"`
	AmountMicro              int64   `json:"amount_micro"`
	Currency                 string  `json:"currency"`
	ProviderDisputeID        string  `json:"provider_dispute_id"`
	UpdatedAt                string  `json:"updated_at,omitempty"`
	ChargebackLedgerEntryIDs []int64 `json:"chargeback_ledger_entry_ids"`
}

// DisputeListResult is GET /api/v1/disputes.
type DisputeListResult struct {
	Disputes []DisputeRowDTO `json:"disputes"`
	Total    int64           `json:"total"`
}

// DisputeLister returns tenant-scoped payment disputes.
type DisputeLister interface {
	ListDisputes(ctx context.Context, customerFilter string, limit, offset int32) (DisputeListResult, error)
}

func (h *BillingHTTPHandlers) registerDisputeRoutes(mux *http.ServeMux) {
	if h.Disputes == nil {
		return
	}
	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	mux.HandleFunc("GET /api/v1/disputes", limit(perm("customers:read", h.listDisputes)))
}

func (h *BillingHTTPHandlers) listDisputes(w http.ResponseWriter, r *http.Request) {
	customerFilter := r.URL.Query().Get("customer_id")
	if h.ResolveDisputeCustomerFilter != nil {
		filter, err := h.ResolveDisputeCustomerFilter(r)
		if err != nil {
			h.writeServiceError(w, err)
			return
		}
		customerFilter = filter
	}

	limit := int32(20)
	offset := int32(0)
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = int32(n)
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = int32(n)
		}
	}
	if limit > 100 {
		limit = 100
	}

	result, err := h.Disputes.ListDisputes(r.Context(), customerFilter, limit, offset)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", st.Message())
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "failed to list disputes")
		return
	}
	httpresponse.JSON(w, http.StatusOK, result)
}

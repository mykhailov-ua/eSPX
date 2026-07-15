package management

import (
	"net/http"
	"strconv"

	"espx/internal/ingestion/sqlc"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// listDisputes handles GET /api/v1/disputes with optional tenant scoping and chargeback ledger IDs.
func (h *Handler) listDisputes(w http.ResponseWriter, r *http.Request) {
	if h.payment == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "PAYMENT_UNAVAILABLE", "payment service not configured")
		return
	}

	u, ok := GetUser(r.Context())
	if !ok {
		httpresponse.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized")
		return
	}

	customerFilter := r.URL.Query().Get("customer_id")
	if u.IsUser() {
		if customerFilter != "" && customerFilter != u.CustomerID.String() {
			httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
			return
		}
		customerFilter = u.CustomerID.String()
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

	resp, err := h.payment.ListDisputes(r.Context(), customerFilter, limit, offset)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", st.Message())
			return
		}
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "failed to list disputes")
		return
	}

	queries := db.New(h.svc.pool)
	rows := make([]map[string]any, 0, len(resp.Disputes))
	for _, d := range resp.Disputes {
		item := map[string]any{
			"intent_id":           d.IntentId,
			"customer_id":         d.CustomerId,
			"amount_micro":        d.AmountMicro,
			"currency":            d.Currency,
			"provider_dispute_id": d.ProviderDisputeId,
		}
		if d.UpdatedAt != nil {
			item["updated_at"] = d.UpdatedAt.AsTime().UTC().Format("2006-01-02T15:04:05Z07:00")
		}
		intentID, parseErr := uuid.Parse(d.IntentId)
		if parseErr == nil {
			ledgerIDs, lerr := queries.ListLedgerChargebackEntryIDs(r.Context(), pgtype.UUID{Bytes: intentID, Valid: true})
			if lerr == nil && len(ledgerIDs) > 0 {
				item["chargeback_ledger_entry_ids"] = ledgerIDs
			} else {
				item["chargeback_ledger_entry_ids"] = []int64{}
			}
		}
		rows = append(rows, item)
	}

	httpresponse.JSON(w, http.StatusOK, map[string]any{
		"disputes": rows,
		"total":    resp.Total,
	})
}

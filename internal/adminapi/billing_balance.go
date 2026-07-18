package adminapi

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"

	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

// BalanceLedgerDTO exposes one balance_ledger row for GET /api/v1/customers/{id}/balance.
type BalanceLedgerDTO struct {
	ID              int64  `json:"id"`
	CustomerID      string `json:"customer_id"`
	CampaignID      string `json:"campaign_id,omitempty"`
	Amount          string `json:"amount"`
	Type            string `json:"type"`
	IdempotencyHash string `json:"idempotency_hash,omitempty"`
	CreatedAt       string `json:"created_at"`
}

// CustomerBalanceDTO is GET /api/v1/customers/{id}/balance.
type CustomerBalanceDTO struct {
	CustomerID string             `json:"customer_id"`
	Balance    string             `json:"balance"`
	Currency   string             `json:"currency"`
	Ledger     []BalanceLedgerDTO `json:"ledger"`
}

// LedgerExportResult captures cursor continuation metadata after a capped CSV stream.
type LedgerExportResult struct {
	NextCursor int64
	Truncated  bool
	Bytes      int
}

// CustomerBalanceReader serves balance JSON and ledger CSV export.
type CustomerBalanceReader interface {
	GetCustomerBalance(ctx context.Context, customerID uuid.UUID) (CustomerBalanceDTO, error)
	ExportCustomerLedgerCSV(ctx context.Context, customerID uuid.UUID, cursor int64, w io.Writer) (LedgerExportResult, error)
}

func (h *BillingHTTPHandlers) registerBalanceRoutes(mux *http.ServeMux) {
	if h.CustomerBalance == nil {
		return
	}
	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	exportLimit := h.LimitExportByCustomer
	if exportLimit == nil {
		exportLimit = limit
	}
	mux.HandleFunc("GET /api/v1/customers/{id}/balance", limit(perm("customers:read", h.getCustomerBalance)))
	mux.HandleFunc("GET /api/v1/customers/{id}/balance/export", limit(exportLimit(perm("customers:read", h.exportCustomerBalance))))
}

func (h *BillingHTTPHandlers) getCustomerBalance(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	if err := h.authorizeCustomer(r, idStr); err != nil {
		h.writeServiceError(w, err)
		return
	}

	report, err := h.CustomerBalance.GetCustomerBalance(r.Context(), customerID)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, report)
}

func (h *BillingHTTPHandlers) exportCustomerBalance(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("format") != "csv" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "format must be csv")
		return
	}

	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	if err := h.authorizeCustomer(r, idStr); err != nil {
		h.writeServiceError(w, err)
		return
	}

	cursor, err := parseExportCursor(r)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")

	var buf bytes.Buffer
	result, err := h.CustomerBalance.ExportCustomerLedgerCSV(r.Context(), customerID, cursor, &buf)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	if result.Truncated {
		w.Header().Set("X-Export-Truncated", "true")
		w.Header().Set("X-Next-Cursor", strconv.FormatInt(result.NextCursor, 10))
	}
	w.Header().Set("X-Export-Bytes", strconv.Itoa(result.Bytes))
	_, _ = w.Write(buf.Bytes())
}

func (h *BillingHTTPHandlers) authorizeCustomer(r *http.Request, customerID string) error {
	if h.AuthorizeCustomerAccess == nil {
		return nil
	}
	return h.AuthorizeCustomerAccess(r, customerID)
}

type invalidExportCursorError string

func errInvalidExportCursor(msg string) error {
	return invalidExportCursorError(msg)
}

func (e invalidExportCursorError) Error() string { return string(e) }

func parseExportCursor(r *http.Request) (int64, error) {
	cursorStr := r.URL.Query().Get("cursor")
	if cursorStr == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseInt(cursorStr, 10, 64)
	if err != nil || cursor < 0 {
		return 0, errInvalidExportCursor("invalid cursor")
	}
	return cursor, nil
}

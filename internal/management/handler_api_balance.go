package management

import (
	"bytes"
	"net/http"
	"strconv"

	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

func (h *Handler) ensureCustomerAccess(r *http.Request, customerID string) error {
	u, ok := GetUser(r.Context())
	if !ok || !u.IsUser() {
		return nil
	}
	cid, err := uuid.Parse(customerID)
	if err != nil {
		return err
	}
	if u.CustomerID != cid {
		return errForbidden
	}
	return nil
}

func parseExportCursor(r *http.Request) (int64, error) {
	cursorStr := r.URL.Query().Get("cursor")
	if cursorStr == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseInt(cursorStr, 10, 64)
	if err != nil || cursor < 0 {
		return 0, errInvalidQuery("invalid cursor")
	}
	return cursor, nil
}

func (h *Handler) getCustomerBalance(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	customerID, err := uuid.Parse(idStr)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	if err := h.ensureCustomerAccess(r, idStr); err != nil {
		writeServiceError(w, err)
		return
	}

	report, err := h.svc.GetCustomerBalance(r.Context(), customerID)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	httpresponse.JSON(w, http.StatusOK, report)
}

func (h *Handler) exportCustomerBalance(w http.ResponseWriter, r *http.Request) {
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

	if err := h.ensureCustomerAccess(r, idStr); err != nil {
		writeServiceError(w, err)
		return
	}

	cursor, err := parseExportCursor(r)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")

	var buf bytes.Buffer
	result, err := h.svc.ExportCustomerLedgerCSV(r.Context(), customerID, cursor, &buf)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	if result.Truncated {
		w.Header().Set("X-Export-Truncated", "true")
		w.Header().Set("X-Next-Cursor", strconv.FormatInt(result.NextCursor, 10))
	}
	w.Header().Set("X-Export-Bytes", strconv.Itoa(result.Bytes))
	_, _ = w.Write(buf.Bytes())
}

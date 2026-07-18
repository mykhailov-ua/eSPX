package adminapi

import (
	"errors"
	"espx/internal/billing"
	billingpb "espx/internal/billing/pb"
	"net/http"
	"strconv"
	"time"

	"espx/pkg/coldpath"

	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

// BillingHTTPHandlers serves M2.8 billing JSON routes.
type BillingHTTPHandlers struct {
	InvoiceGRPC             InvoiceGRPCClient
	InProcessInvoices       InProcessInvoiceService
	CompositeReads          *CompositeReadService
	InvoiceDelivery         InvoiceRetryer
	VoidAuditor             VoidAuditor
	ApplyRateLimit          func(http.HandlerFunc) http.HandlerFunc
	RequirePermission       func(string, http.HandlerFunc) http.HandlerFunc
	AuthorizeCustomerAccess func(*http.Request, string) error
	WriteServiceError       func(http.ResponseWriter, error)
	RequestIsFromAdmin      func(*http.Request) bool

	ApplySelfServeRateLimit    func(http.HandlerFunc) http.HandlerFunc
	RequireSelfServePermission func(string, http.HandlerFunc) http.HandlerFunc
	ResolveSelfServeCustomerID func(*http.Request) (uuid.UUID, error)

	CustomerBalance              CustomerBalanceReader
	Disputes                     DisputeLister
	LimitExportByCustomer        func(http.HandlerFunc) http.HandlerFunc
	ResolveDisputeCustomerFilter func(*http.Request) (string, error)
}

// Register mounts billing admin routes on mux.
func (h *BillingHTTPHandlers) Register(mux *http.ServeMux) {
	if h == nil {
		return
	}
	limit := h.ApplyRateLimit
	perm := h.RequirePermission
	if limit == nil {
		limit = func(next http.HandlerFunc) http.HandlerFunc { return next }
	}
	if perm == nil {
		perm = func(_ string, next http.HandlerFunc) http.HandlerFunc { return next }
	}

	mux.HandleFunc("GET /api/v1/billing/invoices", limit(perm("customers:read", h.listInvoices)))
	mux.HandleFunc("GET /api/v1/billing/invoices/{id}", limit(perm("customers:read", h.getInvoice)))
	mux.HandleFunc("GET /api/v1/billing/invoices/{id}/pdf", limit(perm("customers:read", h.getInvoicePDF)))
	mux.HandleFunc("GET /api/v1/customers/{id}/billing/statement", limit(perm("customers:read", h.getStatement)))
	mux.HandleFunc("GET /api/v1/billing/invoices/{id}/ledger-lines", limit(perm("customers:read", h.getLedgerLines)))
	mux.HandleFunc("POST /api/v1/billing/invoices/preview", limit(perm("customers:read", h.previewInvoice)))
	mux.HandleFunc("GET /api/v1/customers/{id}/wallet", limit(perm("customers:read", h.getWallet)))
	mux.HandleFunc("GET /api/v1/billing/invoices/{id}/deliveries", limit(perm("customers:read", h.listDeliveries)))
	mux.HandleFunc("POST /api/v1/billing/invoices/{id}/deliveries/retry", limit(perm("customers:write", h.retryDelivery)))
	mux.HandleFunc("GET /api/v1/billing/invariant", limit(perm("customers:read", h.getInvariant)))
	mux.HandleFunc("GET /api/v1/billing/summary", limit(perm("shards:read", h.getSummary)))
	mux.HandleFunc("GET /api/v1/customers/{id}/tax-profile", limit(perm("customers:read", h.getTaxProfile)))
	mux.HandleFunc("PUT /api/v1/customers/{id}/tax-profile", limit(perm("customers:write", h.putTaxProfile)))
	mux.HandleFunc("POST /api/v1/billing/invoices/{id}/void", limit(perm("customers:write", h.voidInvoice)))
	mux.HandleFunc("GET /api/v1/customers/{id}/billing/forecast", limit(perm("customers:read", h.getForecast)))

	if h.RequireSelfServePermission != nil && h.ResolveSelfServeCustomerID != nil {
		ssLimit := h.ApplySelfServeRateLimit
		if ssLimit == nil {
			ssLimit = limit
		}
		mux.HandleFunc("GET /api/v1/selfserve/billing/statement", ssLimit(h.RequireSelfServePermission("customers:read", h.getSelfServeStatement)))
	}

	h.registerBalanceRoutes(mux)
	h.registerDisputeRoutes(mux)
}

func (h *BillingHTTPHandlers) listInvoices(w http.ResponseWriter, r *http.Request) {
	if h.InvoiceGRPC == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing service not configured")
		return
	}

	customerRaw := r.URL.Query().Get("customer_id")
	adminList := h.RequestIsFromAdmin != nil && h.RequestIsFromAdmin(r) && customerRaw == ""
	if !adminList {
		if customerRaw == "" {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
			return
		}
		if err := h.authorizeCustomerAccess(r, customerRaw); err != nil {
			h.writeServiceError(w, err)
			return
		}
	} else if customerRaw != "" {
		if err := h.authorizeCustomerAccess(r, customerRaw); err != nil {
			h.writeServiceError(w, err)
			return
		}
	}

	limit, offset := parsePagination(r)
	if adminList {
		h.listInvoicesAdmin(w, r, limit, offset)
		return
	}

	resp, err := h.InvoiceGRPC.ListInvoices(r.Context(), customerRaw, limit, offset)
	if err != nil {
		WriteBillingGRPCError(w, err)
		return
	}
	items := make([]map[string]any, 0, len(resp.Invoices))
	for _, inv := range resp.Invoices {
		items = append(items, invoiceToJSON(inv))
	}
	httpresponse.JSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  resp.Total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *BillingHTTPHandlers) listInvoicesAdmin(w http.ResponseWriter, r *http.Request, limit, offset int32) {
	if h.CompositeReads == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing composite reads not configured")
		return
	}
	filters := AdminInvoiceFilters{}
	if raw := r.URL.Query().Get("customer_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
			return
		}
		filters.CustomerID = &id
	}
	if raw := r.URL.Query().Get("month"); raw != "" {
		month, err := time.Parse("2006-01", raw)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "month must be YYYY-MM")
			return
		}
		filters.Month = &month
	}
	filters.Status = r.URL.Query().Get("status")
	if raw := r.URL.Query().Get("min_total"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid min_total")
			return
		}
		filters.MinTotal = n
	}
	result, err := h.CompositeReads.ListInvoicesAdmin(r.Context(), filters, limit, offset)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, result)
}

func (h *BillingHTTPHandlers) getForecast(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing composite reads not configured")
		return
	}
	customerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	if err := h.authorizeCustomerAccess(r, customerID.String()); err != nil {
		h.writeServiceError(w, err)
		return
	}
	forecast, err := h.CompositeReads.BuildForecast(r.Context(), customerID)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, forecast)
}

func (h *BillingHTTPHandlers) getSelfServeStatement(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil || h.ResolveSelfServeCustomerID == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing not configured")
		return
	}
	customerID, err := h.ResolveSelfServeCustomerID(r)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	from, to, err := ParseStatementPeriod(r.URL.Query().Get("from"), r.URL.Query().Get("to"), r.URL.Query().Get("month"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	stmt, err := h.CompositeReads.BuildStatement(r.Context(), customerID, from, to)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, stmt)
}

func (h *BillingHTTPHandlers) getInvoice(w http.ResponseWriter, r *http.Request) {
	if h.InvoiceGRPC == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing service not configured")
		return
	}
	invoiceID := r.PathValue("id")
	if _, err := uuid.Parse(invoiceID); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid invoice id")
		return
	}
	invoice, err := h.InvoiceGRPC.GetInvoice(r.Context(), invoiceID)
	if err != nil {
		WriteBillingGRPCError(w, err)
		return
	}
	if err := h.authorizeCustomerAccess(r, invoice.CustomerId); err != nil {
		h.writeServiceError(w, err)
		return
	}
	body := invoiceToJSON(invoice)
	body["pdf_url"] = invoicePDFPath(invoiceID)
	httpresponse.JSON(w, http.StatusOK, body)
}

func (h *BillingHTTPHandlers) getInvoicePDF(w http.ResponseWriter, r *http.Request) {
	if h.InvoiceGRPC == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing service not configured")
		return
	}
	invoiceID := r.PathValue("id")
	if _, err := uuid.Parse(invoiceID); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid invoice id")
		return
	}
	invoice, err := h.InvoiceGRPC.GetInvoice(r.Context(), invoiceID)
	if err != nil {
		WriteBillingGRPCError(w, err)
		return
	}
	if err := h.authorizeCustomerAccess(r, invoice.CustomerId); err != nil {
		h.writeServiceError(w, err)
		return
	}
	pdf := billing.RenderInvoicePDF(invoice)
	if len(pdf) == 0 {
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "failed to render invoice pdf")
		return
	}
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(pdf)
}

func (h *BillingHTTPHandlers) getStatement(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing composite reads not configured")
		return
	}
	customerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	if err := h.authorizeCustomerAccess(r, customerID.String()); err != nil {
		h.writeServiceError(w, err)
		return
	}
	from, to, err := ParseStatementPeriod(r.URL.Query().Get("from"), r.URL.Query().Get("to"), r.URL.Query().Get("month"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}
	stmt, err := h.CompositeReads.BuildStatement(r.Context(), customerID, from, to)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, stmt)
}

func (h *BillingHTTPHandlers) getLedgerLines(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil || h.InvoiceGRPC == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing not configured")
		return
	}
	invoiceID := r.PathValue("id")
	invoice, err := h.InvoiceGRPC.GetInvoice(r.Context(), invoiceID)
	if err != nil {
		WriteBillingGRPCError(w, err)
		return
	}
	if err := h.authorizeCustomerAccess(r, invoice.CustomerId); err != nil {
		h.writeServiceError(w, err)
		return
	}
	customerID, _ := uuid.Parse(invoice.CustomerId)
	month := time.Now().UTC()
	if invoice.BillingMonth != nil {
		month = invoice.BillingMonth.AsTime().UTC()
	}
	var cursorID int64
	if c := r.URL.Query().Get("cursor"); c != "" {
		cursorID, _ = strconv.ParseInt(c, 10, 64)
	}
	limit, _ := parsePagination(r)
	lines, nextCursor, total, err := h.CompositeReads.ListLedgerLines(r.Context(), customerID, month, cursorID, limit)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]any{
		"items":       lines,
		"total":       total,
		"next_cursor": nextCursor,
		"limit":       limit,
	})
}

type previewRequest struct {
	CustomerID   string `json:"customer_id"`
	BillingMonth string `json:"billing_month"`
}

func (h *BillingHTTPHandlers) previewInvoice(w http.ResponseWriter, r *http.Request) {
	if h.InProcessInvoices == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing service not configured")
		return
	}
	body, err := coldpath.ReadLimitedBody(w, r, coldpath.DefaultMaxBody)
	if err != nil {
		return
	}
	req, err := coldpath.DecodeBody[previewRequest](body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	if req.CustomerID == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "customer_id is required")
		return
	}
	if err := h.authorizeCustomerAccess(r, req.CustomerID); err != nil {
		h.writeServiceError(w, err)
		return
	}
	customerID, err := uuid.Parse(req.CustomerID)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
		return
	}
	month, err := time.Parse("2006-01", req.BillingMonth)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "billing_month must be YYYY-MM")
		return
	}
	preview, err := h.InProcessInvoices.PreviewInvoice(r.Context(), customerID, month)
	if err != nil {
		writeBillingLocalError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, preview)
}

func (h *BillingHTTPHandlers) getWallet(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing composite reads not configured")
		return
	}
	customerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	if err := h.authorizeCustomerAccess(r, customerID.String()); err != nil {
		h.writeServiceError(w, err)
		return
	}
	wallet, err := h.CompositeReads.GetWallet(r.Context(), customerID)
	if err != nil {
		writeBillingLocalError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, wallet)
}

func (h *BillingHTTPHandlers) listDeliveries(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil || h.InvoiceGRPC == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing not configured")
		return
	}
	invoiceID := r.PathValue("id")
	invoice, err := h.InvoiceGRPC.GetInvoice(r.Context(), invoiceID)
	if err != nil {
		WriteBillingGRPCError(w, err)
		return
	}
	if err := h.authorizeCustomerAccess(r, invoice.CustomerId); err != nil {
		h.writeServiceError(w, err)
		return
	}
	rows, err := h.CompositeReads.ListDeliveries(r.Context(), invoiceID)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]any{"items": rows})
}

func (h *BillingHTTPHandlers) retryDelivery(w http.ResponseWriter, r *http.Request) {
	if h.InvoiceGRPC == nil || h.InvoiceDelivery == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "invoice retry not configured")
		return
	}
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "Idempotency-Key header is required")
		return
	}
	invoiceID := r.PathValue("id")
	invoice, err := h.InvoiceGRPC.GetInvoice(r.Context(), invoiceID)
	if err != nil {
		WriteBillingGRPCError(w, err)
		return
	}
	if err := h.authorizeCustomerAccess(r, invoice.CustomerId); err != nil {
		h.writeServiceError(w, err)
		return
	}
	if err := h.InvoiceDelivery.RetryInvoiceDelivery(r.Context(), invoice, idempotencyKey); err != nil {
		h.writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *BillingHTTPHandlers) getInvariant(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing composite reads not configured")
		return
	}
	var customerID *uuid.UUID
	if raw := r.URL.Query().Get("customer_id"); raw != "" {
		if err := h.authorizeCustomerAccess(r, raw); err != nil {
			h.writeServiceError(w, err)
			return
		}
		id, err := uuid.Parse(raw)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer_id")
			return
		}
		customerID = &id
	} else if h.RequestIsFromAdmin == nil || !h.RequestIsFromAdmin(r) {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "customer_id required for tenant users")
		return
	}
	result, err := h.CompositeReads.GetInvariant(r.Context(), customerID)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, result)
}

func (h *BillingHTTPHandlers) getSummary(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing composite reads not configured")
		return
	}
	if h.RequestIsFromAdmin != nil && !h.RequestIsFromAdmin(r) {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "admin only")
		return
	}
	summary, err := h.CompositeReads.GetSummary(r.Context())
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, summary)
}

func (h *BillingHTTPHandlers) getTaxProfile(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing composite reads not configured")
		return
	}
	customerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	if err := h.authorizeCustomerAccess(r, customerID.String()); err != nil {
		h.writeServiceError(w, err)
		return
	}
	profile, err := h.CompositeReads.GetTaxProfile(r.Context(), customerID)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, profile)
}

func (h *BillingHTTPHandlers) putTaxProfile(w http.ResponseWriter, r *http.Request) {
	if h.CompositeReads == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing composite reads not configured")
		return
	}
	customerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}
	if err := h.authorizeCustomerAccess(r, customerID.String()); err != nil {
		h.writeServiceError(w, err)
		return
	}
	body, err := coldpath.ReadLimitedBody(w, r, coldpath.DefaultMaxBody)
	if err != nil {
		return
	}
	dto, err := coldpath.DecodeBody[TaxProfileDTO](body)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	profile, err := h.CompositeReads.UpsertTaxProfile(r.Context(), customerID, dto)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, profile)
}

func (h *BillingHTTPHandlers) voidInvoice(w http.ResponseWriter, r *http.Request) {
	if h.InProcessInvoices == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing service not configured")
		return
	}
	invoiceID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid invoice id")
		return
	}
	var customerID string
	if h.InvoiceGRPC != nil {
		if inv, gerr := h.InvoiceGRPC.GetInvoice(r.Context(), invoiceID.String()); gerr == nil {
			customerID = inv.CustomerId
			if err := h.authorizeCustomerAccess(r, customerID); err != nil {
				h.writeServiceError(w, err)
				return
			}
		}
	}
	if err := h.InProcessInvoices.VoidInvoice(r.Context(), invoiceID); err != nil {
		writeBillingLocalError(w, err)
		return
	}
	if h.VoidAuditor != nil && customerID != "" {
		_ = h.VoidAuditor.AuditInvoiceVoid(r.Context(), invoiceID.String(), customerID)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *BillingHTTPHandlers) authorizeCustomerAccess(r *http.Request, customerID string) error {
	if h.AuthorizeCustomerAccess == nil {
		return nil
	}
	return h.AuthorizeCustomerAccess(r, customerID)
}

func (h *BillingHTTPHandlers) writeServiceError(w http.ResponseWriter, err error) {
	var cur invalidExportCursorError
	if errors.As(err, &cur) {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", string(cur))
		return
	}
	if errors.Is(err, ErrForbidden) {
		httpresponse.Error(w, http.StatusForbidden, "FORBIDDEN", "forbidden")
		return
	}
	if h.WriteServiceError != nil {
		h.WriteServiceError(w, err)
		return
	}
	httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "request failed")
}

func invoicePDFPath(invoiceID string) string {
	return "/api/v1/billing/invoices/" + invoiceID + "/pdf"
}

// InvoiceToJSON maps a gRPC invoice to JSON-friendly fields for API responses.
func InvoiceToJSON(invoice *billingpb.Invoice) map[string]any {
	return invoiceToJSON(invoice)
}

func invoiceToJSON(invoice *billingpb.Invoice) map[string]any {
	if invoice == nil {
		return nil
	}
	month := ""
	if invoice.BillingMonth != nil {
		month = invoice.BillingMonth.AsTime().UTC().Format("2006-01")
	}
	return map[string]any{
		"id":             invoice.Id,
		"customer_id":    invoice.CustomerId,
		"billing_month":  month,
		"subtotal_micro": invoice.SubtotalMicro,
		"tax_micro":      invoice.TaxMicro,
		"total_micro":    invoice.TotalMicro,
		"currency":       invoice.Currency,
		"tax_scheme":     invoice.TaxScheme,
		"tax_rate_bps":   invoice.TaxRateBps,
		"lines":          invoice.Lines,
	}
}

func parsePagination(r *http.Request) (int32, int32) {
	limit := int32(50)
	offset := int32(0)
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = int32(n)
		}
	}
	if limit > 1000 {
		limit = 1000
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = int32(n)
		}
	}
	return limit, offset
}

func writeBillingLocalError(w http.ResponseWriter, err error) {
	if errors.Is(err, billing.ErrCustomerNotFound) || errors.Is(err, billing.ErrInvoiceNotFound) {
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}
	if errors.Is(err, billing.ErrLedgerDrift) {
		httpresponse.Error(w, http.StatusConflict, "LEDGER_DRIFT", err.Error())
		return
	}
	WriteBillingGRPCError(w, err)
}

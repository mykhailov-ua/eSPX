package management

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"espx/internal/billing"
	billingpb "espx/internal/billing/pb"
	"espx/pkg/coldpath"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getCustomerBillingDashboard handles GET /admin/customers/{id}/billing as an HTMX fragment.
func (handler *Handler) getCustomerBillingDashboard(w http.ResponseWriter, r *http.Request) {
	if handler.billing == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing service not configured")
		return
	}

	customerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	resp, err := handler.billing.ListInvoices(r.Context(), customerID.String(), 50, 0)
	if err != nil {
		writeBillingGRPCError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(renderBillingDashboard(customerID.String(), resp.Invoices)))
}

type generateInvoiceRequest struct {
	BillingMonth string `json:"billing_month"`
}

// generateCustomerInvoice handles POST /admin/customers/{id}/billing/invoices.
func (handler *Handler) generateCustomerInvoice(w http.ResponseWriter, r *http.Request) {
	if handler.billing == nil {
		httpresponse.Error(w, http.StatusServiceUnavailable, "BILLING_UNAVAILABLE", "billing service not configured")
		return
	}

	customerID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid customer id")
		return
	}

	monthRaw := r.FormValue("billing_month")
	if monthRaw == "" && strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		body, readErr := coldpath.ReadLimitedBody(w, r, 16*1024)
		if readErr != nil {
			return
		}
		if len(body) > 0 {
			req, decodeErr := coldpath.DecodeBody[generateInvoiceRequest](body)
			if decodeErr != nil {
				httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
				return
			}
			monthRaw = req.BillingMonth
		}
	}
	if monthRaw == "" {
		now := time.Now().UTC()
		monthRaw = fmt.Sprintf("%04d-%02d", now.Year(), now.Month())
	}

	billingMonth, err := billing.ParseBillingMonth(monthRaw)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "billing_month must be YYYY-MM")
		return
	}

	invoice, err := handler.billing.GenerateInvoice(r.Context(), customerID.String(), billingMonth)
	if err != nil {
		writeBillingGRPCError(w, err)
		return
	}

	if isHTMXRequest(r) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(renderInvoiceRow(invoice)))
		return
	}

	httpresponse.JSON(w, http.StatusOK, invoiceToJSON(invoice))
}

func isHTMXRequest(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}

func writeBillingGRPCError(w http.ResponseWriter, err error) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.InvalidArgument:
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", st.Message())
			return
		case codes.NotFound:
			httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", st.Message())
			return
		case codes.FailedPrecondition:
			httpresponse.Error(w, http.StatusConflict, "LEDGER_DRIFT", st.Message())
			return
		}
	}
	httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", "billing request failed")
}

func renderBillingDashboard(customerID string, invoices []*billingpb.Invoice) string {
	var builder strings.Builder
	builder.WriteString(`<section id="billing-dashboard" class="billing-panel">`)
	builder.WriteString(`<h2>Billing</h2>`)
	builder.WriteString(`<form hx-post="/admin/customers/`)
	builder.WriteString(customerID)
	builder.WriteString(`/billing/invoices" hx-target="#billing-invoices" hx-swap="afterbegin">`)
	builder.WriteString(`<label>Month <input name="billing_month" type="month" required></label>`)
	builder.WriteString(`<button type="submit">Generate invoice</button>`)
	builder.WriteString(`</form>`)
	builder.WriteString(`<table id="billing-invoices"><thead><tr>`)
	builder.WriteString(`<th>Month</th><th>Subtotal</th><th>Tax</th><th>Total</th><th>Scheme</th>`)
	builder.WriteString(`</tr></thead><tbody>`)
	for _, invoice := range invoices {
		builder.WriteString(renderInvoiceRow(invoice))
	}
	builder.WriteString(`</tbody></table></section>`)
	return builder.String()
}

func renderInvoiceRow(invoice *billingpb.Invoice) string {
	if invoice == nil {
		return ""
	}
	month := ""
	if invoice.BillingMonth != nil {
		t := invoice.BillingMonth.AsTime().UTC()
		month = t.Format("2006-01")
	}
	var builder strings.Builder
	builder.WriteString(`<tr id="invoice-`)
	builder.WriteString(invoice.Id)
	builder.WriteString(`"><td>`)
	builder.WriteString(month)
	builder.WriteString(`</td><td>`)
	builder.WriteString(formatBillingMicro(invoice.SubtotalMicro))
	builder.WriteString(` `)
	builder.WriteString(invoice.Currency)
	builder.WriteString(`</td><td>`)
	builder.WriteString(formatBillingMicro(invoice.TaxMicro))
	builder.WriteString(`</td><td>`)
	builder.WriteString(formatBillingMicro(invoice.TotalMicro))
	builder.WriteString(`</td><td>`)
	builder.WriteString(invoice.TaxScheme)
	if invoice.TaxRateBps > 0 {
		builder.WriteString(` (`)
		builder.WriteString(strconv.FormatInt(int64(invoice.TaxRateBps), 10))
		builder.WriteString(` bps)`)
	}
	builder.WriteString(`</td></tr>`)
	return builder.String()
}

func formatBillingMicro(micro int64) string {
	whole := micro / 1_000_000
	frac := micro % 1_000_000
	if frac < 0 {
		frac = -frac
	}
	return fmt.Sprintf("%d.%06d", whole, frac)
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

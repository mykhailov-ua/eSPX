package payment

import (
	"espx/pkg/coldpath"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// HTMXHandler serves cold-path UI fragments without coupling payment to the management Templ stack.
type HTMXHandler struct {
	service *Service
}

// NewHTMXHandler exposes cold-path UI fragments without pulling Templ or session auth into payment gRPC.
func NewHTMXHandler(service *Service) *HTMXHandler {
	return &HTMXHandler{service: service}
}

// RegisterRoutes keeps payment UI endpoints on the webhook sidecar for local demos and integration tests.
func (h *HTMXHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /ui/payment/topup", h.handleTopupForm)
	mux.HandleFunc("POST /ui/payment/intents", h.handleCreateIntent)
	mux.HandleFunc("GET /ui/payment/intents/{id}", h.handleIntentStatus)
	mux.HandleFunc("GET /ui/payment/return", h.handleCheckoutReturn)
}

// handleTopupForm returns a minimal form fragment so HTMX hosts can embed top-up without a separate frontend build.
func (h *HTMXHandler) handleTopupForm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<form id="payment-topup-form" hx-post="/ui/payment/intents" hx-target="#payment-result" hx-swap="innerHTML">
  <div id="payment-result"></div>
  <label>Amount (USD)<input name="amount" type="number" step="0.01" min="0.01" required></label>
  <input type="hidden" name="customer_id" value="">
  <button type="submit">Continue to checkout</button>
</form>`))
}

// createIntentForm accepts both form posts and JSON because HTMX and API clients share the sidecar.
type createIntentForm struct {
	CustomerID  string  `json:"customer_id"`
	Amount      float64 `json:"amount"`
	AmountMicro *int64  `json:"amount_micro"`
}

// handleCreateIntent drives checkout from form or JSON because the sidecar serves both demo HTML and API clients.
func (htmxHandler *HTMXHandler) handleCreateIntent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := coldpath.ReadLimitedBody(w, r, 16*1024)
	if err != nil {
		WriteHTMX(w, r, StatusValidation, CodeInvalidInput, "Check your payment details and try again.")
		return
	}

	customerID, amountMicro, err := parseCreateIntentInput(r.Header.Get("Content-Type"), body)
	if err != nil {
		WriteHTMXError(w, r, err)
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		idempotencyKey = "ui-" + uuid.New().String()
	}

	result, err := htmxHandler.service.CreatePaymentIntent(r.Context(), customerID, amountMicro, "USD", idempotencyKey, nil)
	if err != nil {
		WriteHTMXError(w, r, err, slog.String("customer_id", customerID.String()))
		return
	}

	intent := result.Intent
	intentID := uuid.UUID(intent.ID.Bytes).String()
	checkoutURL := result.CheckoutURL

	fragment := `<div id="payment-intent" data-intent-id="` + intentID + `" data-checkout-url="` + checkoutURL + `">` +
		`<a href="` + checkoutURL + `">Continue</a>` +
		`<div hx-get="/ui/payment/intents/` + intentID + `" hx-trigger="every 3s" hx-swap="outerHTML"></div>` +
		`</div>`
	WriteHTMXOK(w, r, fragment)
}

// handleIntentStatus polls intent state for HTMX refresh loops after redirect from checkout.
func (h *HTMXHandler) handleIntentStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	intentID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		WriteHTMX(w, r, StatusValidation, CodeInvalidInput, "Check your payment details and try again.")
		return
	}

	intent, err := h.service.GetPaymentIntent(r.Context(), intentID)
	if err != nil {
		WriteHTMXError(w, r, err, slog.String("intent_id", intentID.String()))
		return
	}

	status := string(intent.Status)
	fragment := `<div id="payment-intent" data-intent-id="` + intentID.String() + `" data-status="` + status + `">` +
		`<p>Status: ` + status + `</p></div>`
	WriteHTMXOK(w, r, fragment)
}

// parseCreateIntentInput accepts form and JSON bodies because HTMX posts differ from management admin JSON.
func parseCreateIntentInput(contentType string, body []byte) (uuid.UUID, int64, error) {
	if len(body) == 0 {
		return uuid.Nil, 0, errValidation("invalid request body")
	}

	var form createIntentForm
	if strings.HasPrefix(contentType, "application/json") {
		decoded, err := coldpath.DecodeBody[createIntentForm](body)
		if err != nil {
			return uuid.Nil, 0, errValidation("invalid request body")
		}
		form = decoded
	} else {
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return uuid.Nil, 0, errValidation("invalid request body")
		}
		form.CustomerID = values.Get("customer_id")
		if amountStr := values.Get("amount_micro"); amountStr != "" {
			micro, err := strconv.ParseInt(amountStr, 10, 64)
			if err != nil || micro <= 0 {
				return uuid.Nil, 0, errValidation("amount is required")
			}
			form.AmountMicro = &micro
		} else if amountStr := values.Get("amount"); amountStr != "" {
			amount, err := strconv.ParseFloat(amountStr, 64)
			if err != nil || amount <= 0 {
				return uuid.Nil, 0, errValidation("amount is required")
			}
			form.Amount = amount
		}
	}

	customerID, err := uuid.Parse(form.CustomerID)
	if err != nil || customerID == uuid.Nil {
		return uuid.Nil, 0, errValidation("invalid customer id")
	}

	var amountMicro int64
	switch {
	case form.AmountMicro != nil:
		amountMicro = *form.AmountMicro
	case form.Amount > 0:
		amountMicro = int64(form.Amount * 1_000_000)
	default:
		return uuid.Nil, 0, errValidation("amount is required")
	}
	if amountMicro <= 0 {
		return uuid.Nil, 0, errValidation("amount is required")
	}

	return customerID, amountMicro, nil
}

// validationError tags client input failures for MapHTMXError without leaking internal error types.
type validationError string

// Error implements error so validation failures flow through MapHTMXError without importing gRPC types.
func (e validationError) Error() string { return string(e) }

// errValidation tags client input errors for stable HTMX response mapping.
func errValidation(msg string) error { return validationError(msg) }

package payment

import (
	"fmt"
	"net/http"
	"strings"
)

// handleCheckoutReturn serves the browser landing page after Stripe Checkout (including 3DS).
func (htmxHandler *HTMXHandler) handleCheckoutReturn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	status := strings.TrimSpace(r.URL.Query().Get("status"))
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	title := "Payment"
	body := `<p>Return to your dashboard to confirm balance updates.</p>`

	switch status {
	case "success":
		title = "Payment submitted"
		body = `<p>Thank you. Your payment is processing; balance updates after settlement completes.</p>`
		if sessionID != "" {
			body += fmt.Sprintf(`<p data-stripe-session-id="%s"></p>`, sessionID)
		}
	case "cancelled":
		title = "Payment cancelled"
		body = `<p>Checkout was cancelled. You can try again from your account.</p>`
	default:
		title = "Checkout return"
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta charset="utf-8"><title>%s</title></head><body><h1>%s</h1>%s</body></html>`, title, title, body)
}

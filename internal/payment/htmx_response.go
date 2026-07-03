package payment

import (
	"log/slog"
	"net/http"
	"strings"
)

// WriteHTMXError logs server-side detail while returning a sanitized fragment to the browser.
func WriteHTMXError(w http.ResponseWriter, r *http.Request, err error, logAttrs ...any) {
	status, code, message := MapHTMXError(err)
	if status >= StatusFailed {
		attrs := append([]any{slog.String("error", err.Error())}, logAttrs...)
		slog.Error("payment htmx request failed", attrs...)
	}
	WriteHTMX(w, r, status, code, message)
}

// WriteHTMX returns a machine-readable error fragment via X-Payment-Error-Code for host-page branching.
func WriteHTMX(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Payment-Error-Code", code)
	w.WriteHeader(status)
	_, _ = w.Write([]byte(htmxErrorBody(code, message)))
}

// WriteHTMXOK returns swap-ready HTML because HTMX success paths expect a fragment, not JSON.
func WriteHTMXOK(w http.ResponseWriter, r *http.Request, fragment string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if fragment == "" {
		fragment = `<div id="payment-ok" data-status="ok"></div>`
	}
	_, _ = w.Write([]byte(fragment))
}

// htmxErrorBody embeds data-code on the fragment so hosts can style errors without parsing headers.
func htmxErrorBody(code, message string) string {
	var b strings.Builder
	b.WriteString(`<div id="payment-error" data-code="`)
	b.WriteString(code)
	b.WriteString(`">`)
	b.WriteString(message)
	b.WriteString(`</div>`)
	return b.String()
}

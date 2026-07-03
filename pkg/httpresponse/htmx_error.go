package httpresponse

import (
	"bytes"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
)

var htmxErrorFragmentTemplate = template.Must(template.New("htmx_err").Parse(`
<div class="p-4 mb-4 text-sm rounded-lg bg-neutral-900 text-red-400 border border-red-900/60" role="alert">
    <div class="flex items-center font-semibold mb-1 text-neutral-100">
        <svg class="flex-shrink-0 inline w-4 h-4 mr-2" aria-hidden="true" xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 20 20">
            <path d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.5 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 3 0 1.5 1.5 0 0 1-3 0Zm1.5 11.5a1 1 0 0 1-2 0v-6a1 1 0 0 1 2 0v6Z"/>
        </svg>
        <span>Error {{.Status}} ({{.Code}})</span>
    </div>
    <div class="text-neutral-300">{{.Message}}</div>
</div>
`))

var fullPageErrorTemplate = template.Must(template.New("full_err").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Error {{.Status}} - {{.Message}}</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-neutral-950 text-neutral-100 flex items-center justify-center min-h-screen p-6 font-sans">
    <div class="max-w-md w-full bg-neutral-900 rounded-2xl shadow-xl border border-neutral-800 p-8 text-center">
        <div class="w-16 h-16 bg-neutral-800 rounded-full flex items-center justify-center mx-auto mb-6 text-red-400 border border-neutral-700">
            <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path>
            </svg>
        </div>
        <h1 class="text-6xl font-black text-neutral-100 tracking-tight mb-2">{{.Status}}</h1>
        <h2 class="text-xl font-bold text-neutral-200 mb-4">{{.Code}}</h2>
        <p class="text-neutral-400 leading-relaxed mb-8">{{.Message}}</p>
        <a href="/" class="inline-flex items-center justify-center px-5 py-2.5 text-sm font-semibold text-neutral-950 bg-neutral-100 hover:bg-neutral-200 rounded-xl transition duration-150">
            Return to Safety
        </a>
    </div>
</body>
</html>`))

// errorTemplateData binds handler context into HTML templates without exposing raw request fields.
type errorTemplateData struct {
	Status  int
	Code    string
	Message string
}

// HTMXError returns a fragment for HX-Request and a full page otherwise so admin UI errors stay in-band.
func HTMXError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)

	data := errorTemplateData{
		Status:  status,
		Code:    code,
		Message: message,
	}

	var buf bytes.Buffer
	if r.Header.Get("HX-Request") == "true" {
		if err := htmxErrorFragmentTemplate.Execute(&buf, data); err != nil {

			if _, writeErr := w.Write([]byte(`<div style="color:#f87171;font-weight:bold;background:#171717;border:1px solid #7f1d1d;padding:1rem;border-radius:0.5rem;">Error: ` + message + `</div>`)); writeErr != nil {
				slog.Error("failed to write htmx error fallback response", "error", writeErr)
			}
			return
		}
	} else {
		if err := fullPageErrorTemplate.Execute(&buf, data); err != nil {

			if _, writeErr := w.Write([]byte(`<!DOCTYPE html><html><body style="background:#0a0a0a;color:#f5f5f5;font-family:sans-serif;padding:2rem;"><h1>Error ` + strconv.Itoa(status) + `</h1><p style="color:#a3a3a3;">` + message + `</p></body></html>`)); writeErr != nil {
				slog.Error("failed to write full page error fallback response", "error", writeErr)
			}
			return
		}
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		slog.Error("failed to write htmx error response", "error", err)
	}
}

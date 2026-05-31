// Package httpresponse provides low-level HTTP response helpers for the management
// HTTP layer. Two transport formats are supported:
//
//   - JSON (response.go): standard REST responses. The Error helper builds the
//     structured {"error":{"code":...,"message":...}} envelope using a pooled
//     bytes.Buffer to avoid per-call heap allocation. The content-type header is
//     set via direct slice assignment to avoid the map look-up in Header.Set.
//
//   - HTMX (htmx_error.go): HTML fragment or full-page error responses detected
//     by the presence of the HX-Request header. Fragment responses are injected
//     into the HTMX swap target; full-page responses render a standalone error page.
package httpresponse

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
)

type ErrorDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ErrorResponse struct {
	Error ErrorDTO `json:"error"`
}

var (
	newline               = []byte("\n")
	contentTypeJsonHeader = []string{"application/json"}
	bufferPool            = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
)

// JSON writes status and data serialised as JSON. Content-Type is set by direct
// slice assignment (Header["Content-Type"] = ...) to skip the canonicalization
// look-up in http.Header.Set; valid only because "Content-Type" is already canonical.
func JSON(w http.ResponseWriter, status int, data any) {
	w.Header()["Content-Type"] = contentTypeJsonHeader
	w.WriteHeader(status)
	if data != nil {
		out, err := json.Marshal(data)
		if err == nil {
			_, _ = w.Write(out)
			_, _ = w.Write(newline)
		}
	}
}

// Error writes a structured JSON error envelope using a pooled bytes.Buffer.
// The envelope is built via string concatenation to avoid json.Marshal overhead
// for a fixed-shape payload whose fields are always plain strings.
func Error(w http.ResponseWriter, status int, code, message string) {
	w.Header()["Content-Type"] = contentTypeJsonHeader
	w.WriteHeader(status)

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()

	buf.WriteString(`{"error":{"code":"`)
	buf.WriteString(code)
	buf.WriteString(`","message":"`)
	buf.WriteString(message)
	buf.WriteString(`"}}`)
	buf.WriteByte('\n')

	_, _ = w.Write(buf.Bytes())
	bufferPool.Put(buf)
}

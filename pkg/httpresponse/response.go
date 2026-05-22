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

// JSON serializes payload into standard JSON formatting. Ensuring consistent Content-Type headers across all responses permits automated client-side interceptors to rely on predictable payload schemas.
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

// Error encapsulates domain error details into a uniform DTO. This standardizes error boundaries and prevents raw internal stack traces or database errors from leaking to external consumers.
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

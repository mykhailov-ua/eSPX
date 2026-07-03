package httpresponse

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
)

var (
	newline               = []byte("\n")
	contentTypeJsonHeader = []string{"application/json"}
	bufferPool            = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
)

// JSON writes a standard API envelope; used where reflection cost on success paths is acceptable.
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

// Error builds the error JSON by hand and pools buffers to avoid Marshal on failure paths.
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

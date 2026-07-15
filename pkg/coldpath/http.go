package coldpath

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"espx/pkg/httpresponse"
)

// DefaultMaxBody is the default request body cap for management JSON endpoints.
const DefaultMaxBody = 65536

// ReadLimitedBody reads the request body after applying MaxBytesReader.
func ReadLimitedBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	return io.ReadAll(r.Body)
}

// DecodeBody unmarshals body into T.
func DecodeBody[T any](body []byte) (T, error) {
	var v T
	err := json.Unmarshal(body, &v)
	return v, err
}

// DecodeRequest reads and unmarshals the request body into T.
func DecodeRequest[T any](w http.ResponseWriter, r *http.Request, maxBytes int64) (T, error) {
	body, err := ReadLimitedBody(w, r, maxBytes)
	if err != nil {
		var zero T
		return zero, err
	}
	return DecodeBody[T](body)
}

// WritePaginatedJSON sets X-Total-Count and writes items as JSON.
func WritePaginatedJSON[T any](w http.ResponseWriter, items []T, total int64) {
	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	httpresponse.JSON(w, http.StatusOK, items)
}

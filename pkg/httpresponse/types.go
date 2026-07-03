// Package httpresponse provides cold-path JSON and HTMX error helpers for management APIs.
//
// File layout (single package, transport-prefixed files):
//   - types.go — shared DTOs
//   - json.go — JSON success and error envelopes
//   - htmx.go — HTML fragment and full-page error rendering
package httpresponse

// ErrorDTO is the stable machine-readable code surface clients use for retry and alert routing.
type ErrorDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorResponse wraps ErrorDTO so every management API failure shares one JSON envelope.
type ErrorResponse struct {
	Error ErrorDTO `json:"error"`
}

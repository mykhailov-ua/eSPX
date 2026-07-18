package adminapi

import "errors"

// ErrNotImplemented is returned until saved_report_views persistence lands (M6).
var ErrNotImplemented = errors.New("saved report views not implemented")

// Service will own PG CRUD for saved_report_views (M6 ADM-W5).
type Service struct{}

// NewService returns a views service stub.
func NewService() *Service {
	return &Service{}
}

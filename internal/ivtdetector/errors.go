package ivtdetector

import "errors"

var (
	// ErrOutboxBackpressure is returned when the management outbox queue exceeds the configured ceiling.
	ErrOutboxBackpressure = errors.New("management outbox backpressure: pending queue full")
	// ErrManagementUnavailable is returned when the management blacklist API cannot be reached.
	ErrManagementUnavailable = errors.New("management blacklist API unavailable")
	// ErrInvalidIP is returned when ClickHouse returns an empty or malformed IP address.
	ErrInvalidIP = errors.New("invalid IP address")
)

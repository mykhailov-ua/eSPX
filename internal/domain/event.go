package domain

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// contextKey is a private type so request-scoped values do not collide with other packages' context keys.
type contextKey string

// DeduplicationTokenKey carries a broker idempotency token through ingest so duplicate clicks are rejected once.
const DeduplicationTokenKey contextKey = "dedup_token"

// Event is the pooled ingest record so handlers reuse byte buffers instead of allocating per request.
type Event struct {
	ClickID      string
	CampaignID   uuid.UUID
	UserID       string
	Type         string
	Payload      []byte
	IP           string
	UA           string
	FraudReason  string
	CreatedAt    time.Time
	StringBuffer []byte
}

// Reset clears a pooled event for reuse and drops oversized buffers so the pool does not retain wasted capacity.
func (e *Event) Reset() {
	e.ClickID = ""
	e.CampaignID = uuid.Nil
	e.UserID = ""
	e.Type = ""
	if cap(e.Payload) > 4096 {
		e.Payload = make([]byte, 0, 1024)
	} else {
		e.Payload = e.Payload[:0]
	}
	e.IP = ""
	e.UA = ""
	e.FraudReason = ""
	e.CreatedAt = time.Time{}
	if cap(e.StringBuffer) > 2048 {
		e.StringBuffer = make([]byte, 0, 256)
	} else {
		e.StringBuffer = e.StringBuffer[:0]
	}
}

// EventPool recycles Event values on the ingest hot path to keep allocation rate bounded under load.
var EventPool = sync.Pool{
	New: func() any {
		return &Event{
			Payload:      make([]byte, 0, 1024),
			StringBuffer: make([]byte, 0, 256),
		}
	},
}

// EventStore batches accepted events to ClickHouse so the hot path never blocks on columnar writes.
type EventStore interface {
	StoreBatch(ctx context.Context, events []*Event) error
	Close() error
}

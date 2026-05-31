// Package domain defines Event, the fundamental unit of data flowing through the
// ad tracking pipeline. Events are pooled via EventPool to eliminate per-request
// heap allocation in the processor worker path. The Reset method enforces a
// capacity cap of 4 096 bytes on the Payload slice: oversized payloads indicate
// an abnormal upstream condition and should not be retained in the pool.
package domain

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Event represents a single ad interaction signal (impression, click, conversion)
// as it flows from the Redis Stream through the filter pipeline to PostgreSQL and
// ClickHouse. FraudReason is set by FraudFilter and non-empty events are routed
// to the fraud_events ClickHouse table instead of the primary events tables.
// InsertedToCH prevents duplicate insertions when the processor retries a batch
// that was partially acknowledged by ClickHouse.
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
	InsertedToCH bool
}

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
	e.InsertedToCH = false
}

var EventPool = sync.Pool{
	New: func() any {
		return &Event{
			Payload: make([]byte, 0, 1024),
		}
	},
}

// EventStore abstracts batch persistence for downstream stores (PostgreSQL, ClickHouse).
// StoreBatch must be idempotent; duplicate events identified by click_id are silently
// ignored at the database layer via ON CONFLICT DO NOTHING.
type EventStore interface {
	StoreBatch(ctx context.Context, events []*Event) error
	Close() error
}

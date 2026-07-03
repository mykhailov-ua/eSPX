package domain

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const DeduplicationTokenKey contextKey = "dedup_token"

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

var EventPool = sync.Pool{
	New: func() any {
		return &Event{
			Payload:      make([]byte, 0, 1024),
			StringBuffer: make([]byte, 0, 256),
		}
	},
}

type EventStore interface {
	StoreBatch(ctx context.Context, events []*Event) error
	Close() error
}

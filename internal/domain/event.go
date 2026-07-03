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
	TLSHash      string
	SecCHUA      string
	AcceptLang   string
	FraudReason  string
	FraudScore   uint32
	GhostEvent   bool
	ShadowEvent  bool
	CreatedAt    time.Time
	StringBuffer []byte
	// HotScratch is ads-internal ingest state (fraud accumulator pool pointer); nil before pool return.
	HotScratch any
	// FilterDeadlineMono is a monotonic-ns filter budget set by FilterEngine.Check; 0 means unset.
	FilterDeadlineMono int64
	// IngestGeoResolved marks whether GeoHash/GeoCountry were computed for this request.
	IngestGeoResolved bool
	// GeoHash is the crc32 IEEE country shard used by RTB geo indexing.
	GeoHash uint32
	// GeoCountry is the cached ISO country code from the first GeoIP lookup on this event.
	GeoCountry string
	// ClickIDBuf is a pre-allocated buffer to format generated click IDs without heap allocation.
	ClickIDBuf [36]byte
}

// Reset clears a pooled event for reuse and drops oversized buffers so the pool does not retain wasted capacity.
func (event *Event) Reset() {
	event.ClickID = ""
	event.CampaignID = uuid.Nil
	event.UserID = ""
	event.Type = ""
	if cap(event.Payload) > 4096 {
		event.Payload = make([]byte, 0, 1024)
	} else {
		event.Payload = event.Payload[:0]
	}
	event.IP = ""
	event.UA = ""
	event.TLSHash = ""
	event.SecCHUA = ""
	event.AcceptLang = ""
	event.FraudReason = ""
	event.FraudScore = 0
	event.GhostEvent = false
	event.ShadowEvent = false
	event.CreatedAt = time.Time{}
	event.HotScratch = nil
	event.FilterDeadlineMono = 0
	event.IngestGeoResolved = false
	event.GeoHash = 0
	event.GeoCountry = ""
	if cap(event.StringBuffer) > 2048 {
		event.StringBuffer = make([]byte, 0, 256)
	} else {
		event.StringBuffer = event.StringBuffer[:0]
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
	// StoreBatch flushes pooled events asynchronously to keep ingest latency independent of LSM write amplification.
	StoreBatch(ctx context.Context, events []*Event) error
	// Close drains pending batches during shutdown so telemetry is not lost on process exit.
	Close() error
}

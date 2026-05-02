package ads

import (
	"context"
)

// EventStore defines the interface for persisting ad events.
type EventStore interface {
	// StoreBatch saves a batch of events to the underlying storage.
	StoreBatch(ctx context.Context, events []Event) error
	// Close performs any necessary cleanup.
	Close() error
}

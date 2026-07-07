package management

import (
	"context"

	"espx/pkg/cold"
)

// handleReloadRtbCatalog publishes a Redis signal so trackers rebuild RTB deals and campaign catalog.
func (w *OutboxWorker) handleReloadRtbCatalog(ctx context.Context, payload []byte) error {
	_ = cold.UnmarshalLenient[RtbCatalogReloadPayload](payload)
	return w.svc.PublishRtbCatalogReload(ctx)
}

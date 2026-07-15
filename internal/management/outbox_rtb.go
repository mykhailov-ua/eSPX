package management

import (
	"context"

	"espx/pkg/coldpath"
)

// handleReloadRtbCatalog publishes a Redis signal so trackers rebuild RTB deals and campaign catalog.
func (w *OutboxWorker) handleReloadRtbCatalog(ctx context.Context, payload []byte) error {
	_ = coldpath.UnmarshalLenient[RtbCatalogReloadPayload](payload)
	return w.svc.PublishRtbCatalogReload(ctx)
}

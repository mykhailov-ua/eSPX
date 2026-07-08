package ads

import (
	"context"
	"errors"

	"espx/internal/domain"
)

// ErrConsentDenied is returned when required consent purposes are not granted (M6.3).
var ErrConsentDenied = errors.New("consent not granted")

// ConsentFilter rejects events when campaign-required consent purposes are missing.
type ConsentFilter struct {
	registry domain.CampaignRegistry
	store    *ConsentStore
}

// NewConsentFilter wires campaign consent requirements to the Redis-backed consent cache.
func NewConsentFilter(registry domain.CampaignRegistry, store *ConsentStore) *ConsentFilter {
	return &ConsentFilter{registry: registry, store: store}
}

// Check returns ErrConsentDenied when required purposes are not satisfied; runs before Lua.
func (f *ConsentFilter) Check(ctx context.Context, evt *domain.Event) error {
	if f == nil || f.store == nil || evt == nil {
		return nil
	}
	camp, ok := f.registry.GetCampaign(evt.CampaignID)
	if !ok || camp.RequireConsentPurposes == 0 {
		return nil
	}
	if evt.UserID == "" {
		return ErrConsentDenied
	}
	userPurposes := f.store.PurposesForUser(evt.UserID)
	if (userPurposes & camp.RequireConsentPurposes) != camp.RequireConsentPurposes {
		return ErrConsentDenied
	}
	return nil
}

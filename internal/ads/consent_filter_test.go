package ads

import (
	"context"
	"testing"

	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsentFilter_blocksMissingPurposes(t *testing.T) {
	t.Parallel()
	campID := uuid.New()
	custID := uuid.New()
	registry := NewRegistry(nil)
	registry.Add(campID, custID, nil, "", domain.PacingModeAsap, 0, "UTC", 0, 0, nil)
	snap := registry.campaignMapSnapshot()
	info := snap.byID[campID]
	info.campaign.RequireConsentPurposes = ConsentPurposeAdStorage
	newMap := make(map[uuid.UUID]campaignInfo, len(snap.byID))
	for k, v := range snap.byID {
		newMap[k] = v
	}
	newMap[campID] = info
	registry.data.Store(&campaignMapSnapshot{byID: newMap})

	store := NewConsentStore(nil)
	filter := NewConsentFilter(registry, store)
	evt := &domain.Event{CampaignID: campID, UserID: "user-no-consent", Type: "click"}
	err := filter.Check(context.Background(), evt)
	require.ErrorIs(t, err, ErrConsentDenied)
}

func TestConsentFilter_allowsGrantedPurposes(t *testing.T) {
	t.Parallel()
	campID := uuid.New()
	custID := uuid.New()
	registry := NewRegistry(nil)
	registry.Add(campID, custID, nil, "", domain.PacingModeAsap, 0, "UTC", 0, 0, nil)
	snap := registry.campaignMapSnapshot()
	info := snap.byID[campID]
	info.campaign.RequireConsentPurposes = ConsentPurposeAdStorage
	newMap := make(map[uuid.UUID]campaignInfo, len(snap.byID))
	for k, v := range snap.byID {
		newMap[k] = v
	}
	newMap[campID] = info
	registry.data.Store(&campaignMapSnapshot{byID: newMap})

	store := NewConsentStore(nil)
	hashHex := HashUserIDHex("user-ok")
	store.upsertLocal(hashHex, ConsentPurposeAdStorage)
	filter := NewConsentFilter(registry, store)
	evt := &domain.Event{CampaignID: campID, UserID: "user-ok", Type: "click"}
	assert.NoError(t, filter.Check(context.Background(), evt))
}

func TestClassifyFilterErr_consentDenied(t *testing.T) {
	t.Parallel()
	kind, ok := classifyFilterErr(ErrConsentDenied)
	require.True(t, ok)
	assert.Equal(t, filterRejectConsent, kind)
	assert.Equal(t, 204, filterRejectSpecs[kind].status)
}

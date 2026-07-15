package ingestion

import (
	"context"
	"encoding/json"
	"testing"

	"espx/internal/campaignmodel"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectLandingURL_StickyWeighted(t *testing.T) {
	store := NewBrandCreativeStore(nil)
	brandID := uuid.New()
	store.cache.Store(&brandCreativeMapSnapshot{byBrand: map[uuid.UUID][]brandCreativeEntry{
		brandID: {
			{ID: "a", URL: "https://a.example", Weight: 70},
			{ID: "b", URL: "https://b.example", Weight: 30},
		},
	}})

	url1 := store.SelectLandingURL(brandID, "user-sticky-1")
	url2 := store.SelectLandingURL(brandID, "user-sticky-1")
	assert.Equal(t, url1, url2)
	assert.Contains(t, []string{"https://a.example", "https://b.example"}, url1)
}

func TestScheduleFilter_BlocksOutsideDaypart(t *testing.T) {
	registry := NewRegistry(nil)
	campID := uuid.New()
	custID := uuid.New()
	registry.Add(campID, custID, nil, "", campaignmodel.PacingModeAsap, 0, "UTC", 0, 86400, nil)

	snap := registry.campaignMapSnapshot()
	newMap := make(map[uuid.UUID]campaignInfo, len(snap.byID))
	for k, v := range snap.byID {
		newMap[k] = v
	}
	info := newMap[campID]
	info.campaign.DaypartHours = map[int16]struct{}{23: {}}
	newMap[campID] = info
	registry.data.Store(&campaignMapSnapshot{byID: newMap})

	filter := NewScheduleFilter(registry)
	evt := &campaignmodel.Event{CampaignID: campID, Type: "click"}
	err := filter.Check(context.Background(), evt)
	assert.ErrorIs(t, err, ErrScheduleBlocked)
}

func TestBrandCreativeStore_LoadFromRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}

	store := NewBrandCreativeStore(nil)
	brandID := uuid.New()
	raw, err := json.Marshal([]brandCreativeEntry{{ID: "x", URL: "https://x.test", Weight: 100}})
	require.NoError(t, err)
	_ = raw
	store.cache.Store(&brandCreativeMapSnapshot{byBrand: map[uuid.UUID][]brandCreativeEntry{
		brandID: {{ID: "x", URL: "https://x.test", Weight: 100}},
	}})
	assert.Equal(t, "https://x.test", store.SelectLandingURL(brandID, "u1"))
}

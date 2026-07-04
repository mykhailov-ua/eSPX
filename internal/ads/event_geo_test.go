package ads

import (
	"testing"

	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureIngestGeo_cachesForGeoFilter(t *testing.T) {
	geo := &countingGeoProvider{country: "US"}
	evt := &domain.Event{IP: "8.8.8.8"}

	ensureIngestGeo(geo, evt)
	require.True(t, evt.IngestGeoResolved)
	assert.Equal(t, "US", evt.GeoCountry)
	assert.Equal(t, GeoHashFromCountry("US"), evt.GeoHash)
	assert.Equal(t, 1, geo.calls)

	ensureIngestGeo(geo, evt)
	assert.Equal(t, 1, geo.calls, "second call must reuse cache")

	f := NewGeoFilter(geo, &mockRegistry{})
	campID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	staticCampaign.ID = campID
	staticCampaign.TargetCountries = map[string]struct{}{"US": {}}
	evt.CampaignID = campID
	err := f.Check(t.Context(), evt)
	assert.NoError(t, err)
	assert.Equal(t, 1, geo.calls, "GeoFilter must not lookup again")
}

func TestParseCategoryMask(t *testing.T) {
	assert.Equal(t, uint64(4), parseCategoryMask([]byte(`{"category_mask":4,"bid_micro":100}`)))
}

type countingGeoProvider struct {
	country string
	calls   int
}

func (c *countingGeoProvider) GetCountry(ip string) (string, error) {
	c.calls++
	return c.country, nil
}

func (c *countingGeoProvider) IsAnonymous(ip string) (bool, error) {
	return false, nil
}

func (c *countingGeoProvider) Close() error { return nil }

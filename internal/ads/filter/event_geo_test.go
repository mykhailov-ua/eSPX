package filter

import (
	"github.com/google/uuid"
	"testing"

	"espx/internal/domain"

	adstest "espx/internal/ads/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureIngestGeo_cachesForGeoFilter(t *testing.T) {
	geo := &countingGeoProvider{country: "US"}
	evt := &domain.Event{IP: "8.8.8.8"}

	ensureIngestGeo(geo, evt)
	require.True(t, evt.IngestGeoResolved)
	assert.Equal(t, "US", evt.GeoCountry)
	assert.Equal(t, geoHashFromCountry("US"), evt.GeoHash)
	assert.Equal(t, 1, geo.calls)

	ensureIngestGeo(geo, evt)
	assert.Equal(t, 1, geo.calls, "second call must reuse cache")

	f := NewGeoFilter(geo, &adstest.MockRegistry{})
	campID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	adstest.CachedMockCamp.Store(&domain.Campaign{
		ID:              campID,
		TargetCountries: map[string]struct{}{"US": {}},
	})
	t.Cleanup(func() { adstest.CachedMockCamp.Store(nil) })
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

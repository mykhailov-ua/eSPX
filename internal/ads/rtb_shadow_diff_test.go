package ads

import (
	"testing"

	"espx/internal/config"
	"espx/internal/domain"
	"espx/internal/rtb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRtbShadowDiff_goldenParity(t *testing.T) {
	ResetRtbShadowDiffBuckets()
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)

	winnerID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	clientID := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*domain.Campaign{{ID: winnerID, BudgetLimit: 5000, TargetCountries: map[string]struct{}{"US": {}}}},
		map[uuid.UUID]RtbCampaignInput{
			winnerID: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: geo, Weight: 1},
		},
	)

	proc := trackProcessor{rtbCatalog: catalog, rtbMode: rtbModeShadow, ingestGeo: &staticGeoProvider{country: "US"}}

	evtMatch := &domain.Event{CampaignID: winnerID, IP: "8.8.8.8"}
	ensureIngestGeo(proc.ingestGeo, evtMatch)
	_, _ = applyRtbAuction(proc, evtMatch, nil)

	evtMismatch := &domain.Event{CampaignID: clientID, IP: "8.8.8.8"}
	ensureIngestGeo(proc.ingestGeo, evtMismatch)
	_, _ = applyRtbAuction(proc, evtMismatch, nil)

	snap := RtbShadowDiffForWindow(0)
	require.Equal(t, uint64(2), snap.ShadowEvals)
	assert.Equal(t, uint64(1), snap.ShadowWinnerMatch)
	assert.Equal(t, uint64(1), snap.ShadowMismatch)
	assert.Equal(t, uint64(2), snap.LiveWouldAccept)
	assert.Equal(t, float64(0.5), snap.MismatchRate)
}

func TestBudgetAuthorityFromSettings_luaVsRtb(t *testing.T) {
	cfg := &config.Config{RtbMode: "live", RtbBudgetAuthority: "redis"}
	assert.Equal(t, BudgetAuthorityRedis, BudgetAuthorityFromSettings(cfg, "lua"))
	assert.Equal(t, BudgetAuthorityRTB, BudgetAuthorityFromSettings(cfg, "rtb"))
	assert.False(t, RtbSkipLuaBudgetDebit(cfg, "lua"))
	assert.True(t, RtbSkipLuaBudgetDebit(cfg, "rtb"))
}

func TestNormalizeRtbBudgetAuthoritySetting(t *testing.T) {
	v, err := NormalizeRtbBudgetAuthoritySetting("redis")
	require.NoError(t, err)
	assert.Equal(t, "lua", v)
	_, err = NormalizeRtbBudgetAuthoritySetting("bogus")
	assert.ErrorIs(t, err, ErrInvalidRtbBudgetAuthority)
}

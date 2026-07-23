package dedupkey

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatCanonical_GoldenVector(t *testing.T) {
	region := RegionUUID(1)
	source := SyncWorkerSourceID(2, uuid.MustParse("11111111-1111-1111-1111-111111111111"))
	scope := Scope{
		RegionID:    region,
		SourceID:    source,
		SourceEpoch: 7,
		SeqStart:    100,
		SeqEnd:      100,
	}
	factorU := FactorU(CanonicalSpendPayload([]SpendPair{{
		CampaignID:  uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		AmountMicro: 250_000,
	}}))
	factorD := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	key := FormatCanonical(scope, factorU, factorD)
	parsedScope, parsedU, parsedD, err := ParseCanonical(key)
	require.NoError(t, err)
	assert.Equal(t, scope, parsedScope)
	assert.Equal(t, factorU, parsedU)
	assert.Equal(t, factorD, parsedD)

	// Same batch must produce identical SSID + factor_u.
	key2 := FormatCanonical(scope, factorU, factorD)
	assert.Equal(t, key, key2)
	assert.Equal(t, factorU, FactorU(CanonicalSpendPayload([]SpendPair{{
		CampaignID:  uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		AmountMicro: 250_000,
	}})))
}

func TestCanonicalSpendPayload_Sorted(t *testing.T) {
	a := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	b := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	u1 := FactorU(CanonicalSpendPayload([]SpendPair{
		{CampaignID: b, AmountMicro: 1},
		{CampaignID: a, AmountMicro: 2},
	}))
	u2 := FactorU(CanonicalSpendPayload([]SpendPair{
		{CampaignID: a, AmountMicro: 2},
		{CampaignID: b, AmountMicro: 1},
	}))
	assert.Equal(t, u1, u2)
}

func TestRegionUUID_Deterministic(t *testing.T) {
	assert.Equal(t, RegionUUID(1), RegionUUID(1))
	assert.NotEqual(t, RegionUUID(1), RegionUUID(2))
}

package dedup_test

import (
	"context"
	"testing"

	"espx/internal/database"
	"espx/internal/dedup"
	"espx/pkg/dedupkey"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedupClaimConfirm_GoMatchesPGFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	adapter := dedup.NewAdapter(pool, 1, 42)
	scope := adapter.RegionScope(dedupkey.RelaySourceID(1), 99, 99)
	factorU := dedupkey.FactorU(dedupkey.CanonicalRelayPayload(99, "CREATE_CAMPAIGN", []byte(`{"x":1}`)))

	result, err := adapter.ClaimConfirm(ctx, scope, factorU)
	require.NoError(t, err)
	assert.Equal(t, dedup.OutcomeConfirmed, result.Outcome)
	assert.NotEmpty(t, result.DedupKey)

	expected := dedupkey.FormatCanonical(scope, factorU, result.FactorD)
	assert.Equal(t, expected, result.DedupKey)

	replay, err := adapter.ClaimConfirm(ctx, scope, factorU)
	require.NoError(t, err)
	assert.Equal(t, dedup.OutcomeAlreadyConfirmed, replay.Outcome)
	assert.Equal(t, result.DedupKey, replay.DedupKey)

	badFactor := dedupkey.FactorU([]byte("different-payload"))
	mismatch, err := adapter.ClaimConfirm(ctx, scope, badFactor)
	require.NoError(t, err)
	assert.Equal(t, dedup.OutcomeHashMismatch, mismatch.Outcome)
}

func TestDedupFormatKey_SQLGoldenVector(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	region := dedupkey.RegionUUID(1)
	source := dedupkey.RelaySourceID(1)
	factorU := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	factorD := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	var pgKey string
	err := pool.QueryRow(ctx, `
		SELECT dedup_format_key($1, $2, $3, $4, $5, $6, $7)`,
		region, source, int64(7), int64(10), int64(10), factorU, factorD,
	).Scan(&pgKey)
	require.NoError(t, err)

	goKey := dedupkey.FormatCanonical(dedupkey.Scope{
		RegionID:    region,
		SourceID:    source,
		SourceEpoch: 7,
		SeqStart:    10,
		SeqEnd:      10,
	}, factorU, factorD)
	assert.Equal(t, goKey, pgKey)
}

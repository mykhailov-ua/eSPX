package ingestion

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCampaignRedisKeyCatalog_HashTaggedKeys(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	cat := NewCampaignRedisKeyCatalog()

	fixed := cat.FixedKeys(id)
	require.NotEmpty(t, fixed)
	assert.Equal(t, budgetCampaignKey(id), fixed[0])
	assert.Contains(t, fixed[0], "{"+id.String()+"}")

	prefixes := cat.PrefixPatterns(id)
	require.NotEmpty(t, prefixes)
	for _, p := range prefixes {
		assert.Contains(t, p, "{"+id.String()+"}", "prefix %q must include hash tag", p)
	}

	required := cat.ActivationRequiredKeys(id)
	require.Len(t, required, 1)
	assert.Equal(t, budgetCampaignKey(id), required[0])
}

func TestCampaignRedisKeyCatalog_SourceOnlyFence(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	cat := NewCampaignRedisKeyCatalog()
	sourceOnly := cat.SourceOnlyKeys(id)
	require.Len(t, sourceOnly, 1)
	assert.Equal(t, MigrationFenceRedisKey(id), sourceOnly[0])

	fixed := cat.FixedKeys(id)
	for _, key := range fixed {
		assert.NotEqual(t, MigrationFenceRedisKey(id), key)
	}
}

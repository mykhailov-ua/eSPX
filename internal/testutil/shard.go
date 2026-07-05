package testutil

import (
	"testing"

	"espx/internal/ads"

	"github.com/google/uuid"
)

// CampaignIDForShard returns a UUID that sharder routes to wantShard.
func CampaignIDForShard(t testing.TB, sharder ads.Sharder, wantShard int) uuid.UUID {
	t.Helper()
	for range 20_000 {
		id := uuid.New()
		if sharder.GetShard(id) == wantShard {
			return id
		}
	}
	t.Fatalf("could not find campaign id for shard %d", wantShard)
	return uuid.Nil
}

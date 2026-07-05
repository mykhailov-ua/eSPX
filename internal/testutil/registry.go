package testutil

import (
	"path/filepath"
	"testing"

	"espx/internal/ads"
	"espx/internal/ads/db"
)

// NewAdsRegistry isolates registry sync from production replica paths.
func NewAdsRegistry(t testing.TB, repo db.Querier) *ads.Registry {
	t.Helper()
	r := ads.NewRegistry(repo)
	r.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	return r
}

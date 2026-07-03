package adstest

import (
	"path/filepath"
	"testing"

	"espx/internal/ads"
	"espx/internal/ads/db"
)

func NewRegistry(t testing.TB, repo db.Querier) *ads.CampaignRegistry {
	t.Helper()
	r := ads.NewRegistry(repo)
	r.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	return r
}

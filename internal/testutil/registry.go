package testutil

import (
	"path/filepath"
	"testing"

	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"
)

// NewAdsRegistry isolates registry sync from production replica paths.
func NewAdsRegistry(t testing.TB, repo db.Querier) *ingestion.Registry {
	t.Helper()
	r := ingestion.NewRegistry(repo)
	r.SetReplicaPath(filepath.Join(t.TempDir(), "campaigns_replica.json"))
	return r
}

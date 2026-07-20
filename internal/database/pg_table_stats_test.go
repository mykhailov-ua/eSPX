package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPgTableStatsCollector_AfterSeed(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx := context.Background()
	pool, cleanup := SetupTestDB(t)
	defer cleanup()

	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance) VALUES
		('00000000-0000-4000-8000-000000000099'::uuid, 'stats-cust', 0)`)
	require.NoError(t, err)

	dead, err := QueryPgTableDeadTuples(ctx, pool)
	require.NoError(t, err)
	require.Contains(t, dead, "balance_ledger")
	require.Contains(t, dead, "campaigns")
	require.Contains(t, dead, "outbox_events")
}

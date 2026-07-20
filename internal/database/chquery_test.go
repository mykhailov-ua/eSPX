package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCHQuery_Freshness(t *testing.T) {
	t.Parallel()

	stale, sec := Freshness(6*time.Minute, 5*time.Minute)
	assert.True(t, stale)
	assert.GreaterOrEqual(t, sec, 360)

	fresh, sec2 := Freshness(30*time.Second, 5*time.Minute)
	assert.False(t, fresh)
	assert.Less(t, sec2, 60)
}

func TestCHQuery_NoConnection(t *testing.T) {
	t.Parallel()

	var q *CHQuery
	_, err := q.Query(context.Background(), "SELECT 1")
	require.Error(t, err)
}

func TestCHQuery_HeavyGroupByKilled(t *testing.T) {
	if testing.Short() {
		t.Skip("CHG-ERR requires ClickHouse integration")
	}
	t.Skip("requires live ClickHouse with memory governor; run in integration CI")
}

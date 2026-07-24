package server

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisOffsetStore_Roundtrip(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	store := NewRedisOffsetStore(rdb)
	ctx := context.Background()

	got, err := store.Commit(ctx, "tracker-logs", "g1", 10)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), got)

	read, err := store.Committed(ctx, "tracker-logs", "g1")
	require.NoError(t, err)
	assert.Equal(t, uint64(10), read)

	got, err = store.Commit(ctx, "tracker-logs", "g1", 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), got)

	got, err = store.Commit(ctx, "tracker-logs", "g1", 25)
	require.NoError(t, err)
	assert.Equal(t, uint64(25), got)

	_, _ = store.Commit(ctx, "tracker-logs", "g2", 15)
	min, ok, err := store.MinCommitted(ctx, "tracker-logs")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(15), min)

	groups, err := store.ListGroups(ctx, "tracker-logs")
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"g1": 25, "g2": 15}, groups)
}

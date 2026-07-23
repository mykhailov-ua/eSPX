package fingerprint

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordAndListRecent(t *testing.T) {
	if testing.Short() {
		t.Skip("redis integration")
	}
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	ctx := context.Background()
	seenAt := time.Unix(1_700_000_000, 0).UTC()

	require.NoError(t, Record(ctx, rdb, Entry{
		IP:      "203.0.113.10",
		TCPHash: 0xdeadbeef,
		TTL:     64,
		Window:  64240,
		MSS:     44,
		SeenAt:  seenAt,
	}))

	entries, err := ListRecent(ctx, rdb, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "203.0.113.10", entries[0].IP)
	assert.Equal(t, uint32(0xdeadbeef), entries[0].TCPHash)
}

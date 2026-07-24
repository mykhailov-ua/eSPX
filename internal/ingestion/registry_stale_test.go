package ingestion

import (
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistryStaleMode_TTL(t *testing.T) {
	r := NewRegistry(nil)
	r.ConfigureStaleMode(50 * time.Millisecond)
	require.False(t, r.IsStaleMode())

	r.MarkPubSubOK()
	require.False(t, r.IsStaleMode())

	time.Sleep(80 * time.Millisecond)
	require.True(t, r.IsStaleMode())

	r.MarkPubSubOK()
	require.False(t, r.IsStaleMode())
}

func TestResolveDebitShard_RerouteToReserve(t *testing.T) {
	sharder := NewStaticSlotSharder(4)
	f := &UnifiedFilter{sharder: sharder, breakers: make([]*database.RedisBreaker, 4)}
	for i := range f.breakers {
		f.breakers[i] = database.NewRedisBreaker(1, 1, time.Hour)
	}
	// Trip shard 0.
	f.breakers[0].RecordFailure()
	require.Equal(t, database.CircuitOpen, f.breakers[0].State())

	campID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	// Force StaticSlot → 0 by picking an ID that maps to shard 0.
	for {
		campID = uuid.New()
		if sharder.GetShard(campID) == 0 {
			break
		}
	}

	camp := &campaignmodel.Campaign{
		HasTriplet:    true,
		PrimaryAShard: 0,
		PrimaryBShard: 1,
		ReserveShard:  2,
	}

	shard, err := f.resolveDebitShard(campID, "user-1", camp)
	require.NoError(t, err)
	assert.NotEqual(t, 0, shard)
	assert.Contains(t, []int{1, 2}, shard)
}

func TestResolveDebitShard_UnavailableWithoutTriplet(t *testing.T) {
	sharder := NewStaticSlotSharder(4)
	f := &UnifiedFilter{sharder: sharder, breakers: make([]*database.RedisBreaker, 4)}
	for i := range f.breakers {
		f.breakers[i] = database.NewRedisBreaker(1, 1, time.Hour)
	}
	f.breakers[0].RecordFailure()

	var campID uuid.UUID
	for {
		campID = uuid.New()
		if sharder.GetShard(campID) == 0 {
			break
		}
	}

	_, err := f.resolveDebitShard(campID, "user", &campaignmodel.Campaign{})
	require.ErrorIs(t, err, ErrShardUnavailable)
}

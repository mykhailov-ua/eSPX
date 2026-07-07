package rtb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDealIndex_UpdateAndLookup(t *testing.T) {
	idx := NewDealIndex()
	idx.UpdateDeals([]DealData{
		{DealID: "deal-a", FloorMicro: 100, GeoMask: 7, CatMask: 3, PacingOpen: PacingOpen, CustomerID: 42},
		{DealID: "deal-b", FloorMicro: 200, PacingOpen: PacingClosed, CustomerID: 99},
	})

	require.Equal(t, 2, idx.Len())
	d, ok := idx.Lookup("deal-a")
	require.True(t, ok)
	assert.Equal(t, int64(100), d.FloorMicro)
	assert.Equal(t, uint64(7), d.GeoMask)
	assert.Equal(t, PacingOpen, d.PacingOpen)

	_, ok = idx.Lookup("missing")
	assert.False(t, ok)

	all := idx.All()
	require.Len(t, all, 2)
}

func TestDealIndex_UpdateNilClears(t *testing.T) {
	idx := NewDealIndex()
	idx.UpdateDeals([]DealData{{DealID: "x", FloorMicro: 1}})
	idx.UpdateDeals(nil)
	assert.Equal(t, 0, idx.Len())
}

func TestDealIndex_UpdateReplacesSnapshot(t *testing.T) {
	idx := NewDealIndex()
	idx.UpdateDeals([]DealData{{DealID: "old", FloorMicro: 1}})
	idx.UpdateDeals([]DealData{{DealID: "new", FloorMicro: 99}})

	_, ok := idx.Lookup("old")
	assert.False(t, ok)
	d, ok := idx.Lookup("new")
	require.True(t, ok)
	assert.Equal(t, int64(99), d.FloorMicro)
}

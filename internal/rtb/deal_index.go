package rtb

import "sync/atomic"

// DealData is the cold-path input shape for PMP deal targeting and floors.
type DealData struct {
	DealID     string
	FloorMicro int64
	GeoMask    uint64
	CatMask    uint64
	PacingOpen uint8
	CustomerID CustomerID
	Seats      int32
}

type dealSnapshot struct {
	byDealID map[string]DealData
	all      []DealData
}

// DealIndex holds an atomically swapped deal catalog for read-only hot-path lookups.
type DealIndex struct {
	snap atomic.Pointer[dealSnapshot]
}

// NewDealIndex creates an empty deal index.
func NewDealIndex() *DealIndex {
	idx := &DealIndex{}
	idx.snap.Store(&dealSnapshot{
		byDealID: make(map[string]DealData),
	})
	return idx
}

// UpdateDeals rebuilds the in-memory deal index from Postgres rows.
func (idx *DealIndex) UpdateDeals(deals []DealData) {
	if deals == nil {
		deals = []DealData{}
	}
	snap := &dealSnapshot{
		byDealID: make(map[string]DealData, len(deals)),
		all:      deals,
	}
	for _, d := range deals {
		if d.DealID == "" {
			continue
		}
		snap.byDealID[d.DealID] = d
	}
	idx.snap.Store(snap)
}

// Lookup returns one deal by deal_id.
func (idx *DealIndex) Lookup(dealID string) (DealData, bool) {
	snap := idx.snap.Load()
	if snap == nil || dealID == "" {
		return DealData{}, false
	}
	d, ok := snap.byDealID[dealID]
	return d, ok
}

// All returns a snapshot slice of every deal.
func (idx *DealIndex) All() []DealData {
	snap := idx.snap.Load()
	if snap == nil || len(snap.all) == 0 {
		return nil
	}
	out := make([]DealData, len(snap.all))
	copy(out, snap.all)
	return out
}

// Len returns the number of indexed deals.
func (idx *DealIndex) Len() int {
	snap := idx.snap.Load()
	if snap == nil {
		return 0
	}
	return len(snap.all)
}

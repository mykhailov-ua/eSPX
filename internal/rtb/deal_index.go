package rtb

import "sync/atomic"

const dealIDMaxLen = 64

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

type dealEntry struct {
	idLen uint8
	id    [dealIDMaxLen]byte
	data  DealData
}

type dealSnapshot struct {
	byDealID map[string]DealData
	all      []DealData
	entries  []dealEntry
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
		entries:  make([]dealEntry, 0, len(deals)),
	}
	for _, d := range deals {
		if d.DealID == "" {
			continue
		}
		snap.byDealID[d.DealID] = d
		var e dealEntry
		ln := len(d.DealID)
		if ln > dealIDMaxLen {
			ln = dealIDMaxLen
		}
		e.idLen = uint8(ln)
		for i := 0; i < ln; i++ {
			e.id[i] = d.DealID[i]
		}
		e.data = d
		snap.entries = append(snap.entries, e)
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

// LookupBytes returns one deal by deal_id bytes without heap allocation.
func (idx *DealIndex) LookupBytes(dealID []byte) (DealData, bool) {
	ln := len(dealID)
	if ln == 0 || ln > dealIDMaxLen {
		return DealData{}, false
	}
	snap := idx.snap.Load()
	if snap == nil {
		return DealData{}, false
	}
	for i := range snap.entries {
		e := &snap.entries[i]
		if int(e.idLen) != ln {
			continue
		}
		if bytesEqual(e.id[:ln], dealID) {
			return e.data, true
		}
	}
	return DealData{}, false
}

func bytesEqual(a, b []byte) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

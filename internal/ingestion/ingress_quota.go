package ingestion

import (
	"sync"
	"sync/atomic"
)

const (
	ingressCacheLine  = 64
	maxIngressWorkers = 64
)

// IngressQuotaCell is a cache-line-isolated per-(shard, worker) ingress counter (section 4).
type IngressQuotaCell struct {
	maxAllowed uint64
	_          [ingressCacheLine - 8]byte
	currentOps atomic.Uint64
	_          [ingressCacheLine - 8]byte
}

type ingressQuotaMap struct {
	epoch      int64
	numShards  uint8
	numWorkers uint8
	cells      []IngressQuotaCell
}

var ingressQuotaMapPool = sync.Pool{
	New: func() any {
		return &ingressQuotaMap{}
	},
}

func buildIngressQuotaMap(epoch int64, limits *UDPControlLimits, numWorkers int) *ingressQuotaMap {
	if limits == nil || limits.NumShards == 0 || numWorkers <= 0 {
		return nil
	}
	if numWorkers > maxIngressWorkers {
		numWorkers = maxIngressWorkers
	}
	n := int(limits.NumShards) * numWorkers
	m := ingressQuotaMapPool.Get().(*ingressQuotaMap)
	if cap(m.cells) < n {
		m.cells = make([]IngressQuotaCell, n)
	} else {
		m.cells = m.cells[:n]
		for i := range m.cells {
			m.cells[i].maxAllowed = 0
			m.cells[i].currentOps.Store(0)
		}
	}
	m.epoch = epoch
	m.numShards = limits.NumShards
	m.numWorkers = uint8(numWorkers)
	for shard := 0; shard < int(limits.NumShards); shard++ {
		limit := limits.Limits[shard]
		perWorker := limit / uint64(numWorkers)
		if perWorker == 0 && limit > 0 {
			perWorker = 1
		}
		base := shard * numWorkers
		for w := 0; w < numWorkers; w++ {
			m.cells[base+w].maxAllowed = perWorker
			m.cells[base+w].currentOps.Store(0)
		}
	}
	return m
}

func (m *ingressQuotaMap) tryAcquire(shard, worker int) bool {
	if m == nil {
		return true
	}
	if shard < 0 || worker < 0 || shard >= int(m.numShards) {
		return true
	}
	if worker >= int(m.numWorkers) {
		worker = worker % int(m.numWorkers)
	}
	idx := shard*int(m.numWorkers) + worker
	if idx >= len(m.cells) {
		return true
	}
	cell := &m.cells[idx]
	if cell.maxAllowed == 0 {
		return true
	}
	ops := cell.currentOps.Add(1)
	if ops > cell.maxAllowed {
		cell.currentOps.Add(^uint64(0))
		return false
	}
	return true
}

// unpaddedIngressCounters is a tight array of atomics (false-sharing baseline).
type unpaddedIngressCounters struct {
	counters [maxIngressWorkers]atomic.Uint64
	max      uint64
}

func (m *unpaddedIngressCounters) tryAcquire(worker int) bool {
	if worker < 0 || worker >= maxIngressWorkers {
		return true
	}
	ops := m.counters[worker].Add(1)
	if ops > m.max {
		m.counters[worker].Add(^uint64(0))
		return false
	}
	return true
}

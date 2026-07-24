package ingestion

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/metrics"

	redis "github.com/redis/go-redis/v9"
)

const (
	fraudAggTableSize     = 4096
	fraudAggTableMask     = fraudAggTableSize - 1
	fraudAggThreshold     = fraudRingUsable * 8 / 10
	fraudAggFlushInterval = 75 * time.Millisecond
	fraudAggMaxProbe      = 64
)

// fraudAggCell is one fixed hash-table bucket keyed by IPv4 /24 + fraud reason id.
type fraudAggCell struct {
	subnetPrefix atomic.Uint32
	reasonID     atomic.Uint32
	count        atomic.Uint64
}

type fraudAggFlushEntry struct {
	subnet uint32
	reason uint8
	count  uint64
}

var fraudAggFlushPool = sync.Pool{
	New: func() any {
		s := make([]fraudAggFlushEntry, 0, 256)
		return &s
	},
}

// fraudAggregateExempt reports L3 blocklist or L1-reject events that must never be aggregated.
func fraudAggregateExempt(evt *campaignmodel.Event) bool {
	if evt == nil || evt.FraudReason == "" {
		return false
	}
	if fraudReasonContainsCode(evt.FraudReason, FraudReasonCodeL3Blocklist) {
		return true
	}
	return countL1HighSignalsInReason(evt.FraudReason) >= 2
}

func countL1HighSignalsInReason(reason string) int {
	n := 0
	for id := FraudReasonID(1); id < fraudReasonCount; id++ {
		if FraudSignalFlags(id)&fraudSignalL1High == 0 {
			continue
		}
		if fraudReasonContainsCode(reason, FraudReasonCode(id)) {
			n++
		}
	}
	return n
}

func fraudReasonContainsCode(reason, code string) bool {
	if reason == "" || code == "" {
		return false
	}
	off := 0
	for off <= len(reason) {
		end := off
		for end < len(reason) && reason[end] != ',' {
			end++
		}
		if len(code) == end-off && reason[off:end] == code {
			return true
		}
		if end >= len(reason) {
			break
		}
		off = end + 1
	}
	return false
}

func primaryFraudReasonID(reason string) uint8 {
	if reason == "" {
		return 0
	}
	end := len(reason)
	for i := 0; i < len(reason); i++ {
		if reason[i] == ',' {
			end = i
			break
		}
	}
	for id := FraudReasonID(1); id < fraudReasonCount; id++ {
		code := FraudReasonCode(id)
		if len(code) == end && reason[:end] == code {
			return uint8(id)
		}
	}
	return 0
}

// ipv4Subnet24Prefix returns the IPv4 /24 network prefix (last octet zeroed).
func ipv4Subnet24Prefix(ip string) (uint32, bool) {
	if len(ip) < 7 {
		return 0, false
	}
	var octets [4]uint32
	idx := 0
	val := uint32(0)
	for i := 0; i < len(ip) && idx < 4; i++ {
		c := ip[i]
		if c >= '0' && c <= '9' {
			val = val*10 + uint32(c-'0')
			if val > 255 {
				return 0, false
			}
			continue
		}
		if c == '.' {
			octets[idx] = val
			idx++
			val = 0
			continue
		}
		return 0, false
	}
	if idx != 3 {
		return 0, false
	}
	octets[3] = val
	addr := (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3]
	return addr & 0xFFFFFF00, true
}

func fraudAggHash(subnet uint32, reason uint8) uint32 {
	h := subnet ^ (uint32(reason) * 0x9e3779b9)
	h ^= h >> 16
	return h & fraudAggTableMask
}

func (q *FraudStreamWriter) ringFill() uint64 {
	alloc := atomic.LoadUint64(&q.allocCursor)
	read := atomic.LoadUint64(&q.readCursor)
	if alloc <= read {
		return 0
	}
	return alloc - read
}

func (q *FraudStreamWriter) publishAggMode(fill uint64) {
	agg := uint32(0)
	if fill >= fraudAggThreshold || atomic.LoadUint32(&q.forceAgg) == 1 {
		agg = 1
	}
	prev := atomic.LoadUint32(&q.aggregating)
	if prev == agg {
		return
	}
	if atomic.CompareAndSwapUint32(&q.aggregating, prev, agg) {
		metrics.FraudStreamMode.WithLabelValues("aggregating").Set(float64(agg))
	}
}

func (q *FraudStreamWriter) aggTableFillRatio() float64 {
	occ := atomic.LoadUint64(&q.aggOccupied)
	return float64(occ) / float64(fraudAggTableSize)
}

func (q *FraudStreamWriter) aggregateEvent(evt *campaignmodel.Event) bool {
	subnet, ok := ipv4Subnet24Prefix(evt.IP)
	if !ok {
		return false
	}
	reasonID := primaryFraudReasonID(evt.FraudReason)
	if reasonID == 0 {
		return false
	}
	if q.aggIncrement(subnet, reasonID) {
		metrics.FraudStreamAggregatedTotal.Inc()
		return true
	}
	metrics.FraudStreamAggregatedDropTotal.Inc()
	return false
}

func (q *FraudStreamWriter) aggIncrement(subnet uint32, reasonID uint8) bool {
	start := fraudAggHash(subnet, reasonID)
	for probe := 0; probe < fraudAggMaxProbe; probe++ {
		idx := (start + uint32(probe)) & fraudAggTableMask
		cell := &q.aggSlots[idx]

		for {
			existing := cell.subnetPrefix.Load()
			if existing == 0 {
				if cell.subnetPrefix.CompareAndSwap(0, subnet) {
					cell.reasonID.Store(uint32(reasonID))
					cell.count.Store(1)
					atomic.AddUint64(&q.aggOccupied, 1)
					metrics.FraudStreamAggTableFill.Set(q.aggTableFillRatio())
					return true
				}
				continue
			}
			if existing == subnet && uint8(cell.reasonID.Load()) == reasonID {
				cell.count.Add(1)
				return true
			}
			break
		}
	}
	return false
}

func (q *FraudStreamWriter) aggregateFlusher() {
	defer q.aggWg.Done()
	ticker := time.NewTicker(fraudAggFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.stopCh:
			q.flushAggregates(true)
			return
		case <-ticker.C:
			q.flushAggregates(false)
		}
	}
}

func (q *FraudStreamWriter) flushAggregates(final bool) {
	entriesPtr := fraudAggFlushPool.Get().(*[]fraudAggFlushEntry)
	entries := *entriesPtr
	entries = entries[:0]

	windowStart := atomic.LoadInt64(&q.aggWindowStart)
	now := time.Now().UnixMilli()
	if windowStart == 0 {
		atomic.StoreInt64(&q.aggWindowStart, now)
		windowStart = now
	}
	windowMs := now - windowStart
	if windowMs <= 0 {
		windowMs = int64(fraudAggFlushInterval / time.Millisecond)
	}

	for i := range q.aggSlots {
		cell := &q.aggSlots[i]
		subnet := cell.subnetPrefix.Load()
		if subnet == 0 {
			continue
		}
		count := cell.count.Swap(0)
		if count == 0 {
			continue
		}
		reason := uint8(cell.reasonID.Load())
		entries = append(entries, fraudAggFlushEntry{
			subnet: subnet,
			reason: reason,
			count:  count,
		})
		if cell.count.Load() == 0 {
			if cell.subnetPrefix.CompareAndSwap(subnet, 0) {
				cell.reasonID.Store(0)
				atomic.AddUint64(&q.aggOccupied, ^uint64(0))
			}
		}
	}
	metrics.FraudStreamAggTableFill.Set(q.aggTableFillRatio())
	atomic.StoreInt64(&q.aggWindowStart, now)

	if len(entries) == 0 {
		*entriesPtr = entries
		fraudAggFlushPool.Put(entriesPtr)
		if final {
			return
		}
		return
	}

	ctx := context.Background()
	pipe := q.rdbs[0].Pipeline()
	for _, e := range entries {
		fillFraudAggregateValues(e, windowMs, q.aggValScratch[:])
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: q.stream,
			MaxLen: q.maxLen,
			Approx: true,
			Values: q.aggValScratch[:],
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		for range entries {
			filterFraudStreamWriteErrors.Inc()
		}
	}

	*entriesPtr = entries
	fraudAggFlushPool.Put(entriesPtr)
}

func fillFraudAggregateValues(e fraudAggFlushEntry, windowMs int64, valSlice []any) {
	subnetStr := formatIPv4Subnet24(e.subnet)
	reasonStr := FraudReasonCode(FraudReasonID(e.reason))
	countStr := strconv.FormatUint(e.count, 10)
	windowStr := strconv.FormatInt(windowMs, 10)

	valSlice[0] = "type"
	valSlice[1] = "fraud_aggregate"
	valSlice[2] = "subnet"
	valSlice[3] = subnetStr
	valSlice[4] = "fraud_reason"
	valSlice[5] = reasonStr
	valSlice[6] = "count"
	valSlice[7] = countStr
	valSlice[8] = "window_ms"
	valSlice[9] = windowStr
}

func formatIPv4Subnet24(prefix uint32) string {
	a := (prefix >> 24) & 0xFF
	b := (prefix >> 16) & 0xFF
	c := (prefix >> 8) & 0xFF
	return strconv.FormatUint(uint64(a), 10) + "." +
		strconv.FormatUint(uint64(b), 10) + "." +
		strconv.FormatUint(uint64(c), 10) + ".0/24"
}

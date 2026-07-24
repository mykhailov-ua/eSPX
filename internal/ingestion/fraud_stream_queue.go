package ingestion

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/ingestion/pb"
	"espx/internal/metrics"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// M14-10: analytical lane 3584 + critical lane 512 (total 4096). Analytical keeps M11 agg at ≥80%.
const (
	fraudAnalyticalCapacity = 3584
	fraudAnalyticalUsable   = fraudAnalyticalCapacity - 1

	fraudCriticalCapacity = 512
	fraudCriticalMask     = fraudCriticalCapacity - 1
	fraudCriticalUsable   = fraudCriticalCapacity - 1
	fraudCriticalSpinMax  = 32

	// Compat aliases used by aggregate threshold and older tests.
	fraudRingCapacity = fraudAnalyticalCapacity
	fraudRingUsable   = fraudAnalyticalUsable

	fraudFlushBatch = 64

	fraudSlotClickMax   = 128
	fraudSlotUserMax    = 128
	fraudSlotTypeMax    = 32
	fraudSlotIPMax      = 64
	fraudSlotUAMax      = 512
	fraudSlotPayloadMax = 2048
	fraudSlotReasonMax  = 128
)

// fraudStreamSlot stores one fraud event in fixed arrays to avoid heap allocations on enqueue.
type fraudStreamSlot struct {
	ready      atomic.Uint32
	shard      uint8
	_          [3]byte
	campaignID uuid.UUID
	createdAt  int64

	clickLen   uint16
	userLen    uint16
	typeLen    uint16
	ipLen      uint16
	uaLen      uint16
	payloadLen uint16
	reasonLen  uint16

	clickID [fraudSlotClickMax]byte
	userID  [fraudSlotUserMax]byte
	evtType [fraudSlotTypeMax]byte
	ip      [fraudSlotIPMax]byte
	ua      [fraudSlotUAMax]byte
	payload [fraudSlotPayloadMax]byte
	reason  [fraudSlotReasonMax]byte

	fraudScore uint32
	ghostEvent bool
}

// FraudStreamWriter decouples fraud telemetry from the gnet hot path via lossy async queues.
type FraudStreamWriter struct {
	_           [64]byte
	writeCursor uint64
	_           [64]byte
	allocCursor uint64
	_           [64]byte
	readCursor  uint64
	_           [64]byte
	slots       [fraudAnalyticalCapacity]fraudStreamSlot

	_         [64]byte
	critWrite uint64
	_         [64]byte
	critAlloc uint64
	_         [64]byte
	critRead  uint64
	_         [64]byte
	critSlots [fraudCriticalCapacity]fraudStreamSlot

	_              [64]byte
	aggregating    uint32
	forceAgg       uint32
	_              [56]byte
	aggOccupied    uint64
	_              [64]byte
	aggWindowStart int64
	_              [56]byte
	aggSlots       [fraudAggTableSize]fraudAggCell
	aggValScratch  [10]any

	stream string
	maxLen int64
	rdbs   []redis.UniversalClient

	stopCh chan struct{}
	wg     sync.WaitGroup
	aggWg  sync.WaitGroup
}

// NewFraudStreamWriter starts the background drainer when Redis and stream name are configured.
func NewFraudStreamWriter(rdbs []redis.UniversalClient, stream string, maxLen int64) *FraudStreamWriter {
	if len(rdbs) == 0 || stream == "" {
		return nil
	}
	q := &FraudStreamWriter{
		stream: stream,
		maxLen: maxLen,
		rdbs:   rdbs,
		stopCh: make(chan struct{}),
	}
	q.wg.Add(1)
	go q.worker()
	q.aggWg.Add(1)
	go q.aggregateFlusher()
	metrics.FraudStreamMode.WithLabelValues("aggregating").Set(0)
	metrics.FraudStreamAggTableFill.Set(0)
	metrics.FraudStreamRingFillRatio.Set(0)
	metrics.FraudStreamPending.Set(0)
	return q
}

// copyFraudField stores fraud strings in fixed ring slots so enqueue avoids heap allocations.
func copyFraudField(dst []byte, s string) int {
	n := len(s)
	if n > len(dst) {
		n = len(dst)
	}
	if n > 0 {
		copy(dst[:n], s[:n])
	}
	return n
}

func fillFraudSlot(slot *fraudStreamSlot, shard int, evt *campaignmodel.Event) {
	slot.ready.Store(0)
	slot.shard = uint8(shard)
	slot.campaignID = evt.CampaignID
	slot.createdAt = evt.CreatedAt.UnixNano()
	slot.clickLen = uint16(copyFraudField(slot.clickID[:], evt.ClickID))
	slot.userLen = uint16(copyFraudField(slot.userID[:], evt.UserID))
	slot.typeLen = uint16(copyFraudField(slot.evtType[:], evt.Type))
	slot.ipLen = uint16(copyFraudField(slot.ip[:], evt.IP))
	slot.uaLen = uint16(copyFraudField(slot.ua[:], evt.UA))
	slot.payloadLen = uint16(copyFraudField(slot.payload[:], unsafeString(evt.Payload)))
	slot.reasonLen = uint16(copyFraudField(slot.reason[:], evt.FraudReason))
	slot.fraudScore = evt.FraudScore
	slot.ghostEvent = evt.GhostEvent
	slot.ready.Store(1)
}

// Enqueue routes critical (L1/L3) events to the reserved lane; others use analytical + M11 agg.
func (q *FraudStreamWriter) Enqueue(shard int, evt *campaignmodel.Event) bool {
	if q == nil || evt == nil {
		return true
	}
	if shard < 0 || shard >= len(q.rdbs) {
		shard = 0
	}

	if fraudAggregateExempt(evt) {
		return q.enqueueCritical(shard, evt)
	}

	fill := q.ringFill()
	q.publishAggMode(fill)
	q.publishRingGauges(fill)
	if fill >= fraudAggThreshold || atomic.LoadUint32(&q.forceAgg) == 1 {
		if q.aggregateEvent(evt) {
			return true
		}
	}
	return q.enqueueAnalytical(shard, evt)
}

// SetForceAggregate enables aggregating=force when fraud consumer lag exceeds threshold (M14-12).
func (q *FraudStreamWriter) SetForceAggregate(force bool) {
	if q == nil {
		return
	}
	want := uint32(0)
	if force {
		want = 1
	}
	prev := atomic.SwapUint32(&q.forceAgg, want)
	if force && prev == 0 {
		metrics.FraudStreamBackpressureTotal.Inc()
		atomic.StoreUint32(&q.aggregating, 1)
		metrics.FraudStreamMode.WithLabelValues("aggregating").Set(1)
	}
	if !force {
		q.publishAggMode(q.ringFill())
	}
}

func (q *FraudStreamWriter) publishRingGauges(fill uint64) {
	metrics.FraudStreamRingFillRatio.Set(float64(fill) / float64(fraudAnalyticalUsable))
	metrics.FraudStreamPending.Set(float64(q.Pending()))
}

func (q *FraudStreamWriter) enqueueCritical(shard int, evt *campaignmodel.Event) bool {
	for {
		alloc := atomic.LoadUint64(&q.critAlloc)
		read := atomic.LoadUint64(&q.critRead)
		if alloc-read >= fraudCriticalUsable {
			for spin := 0; spin < fraudCriticalSpinMax; spin++ {
				if spin < 8 {
					runtime.Gosched()
				} else {
					time.Sleep(time.Microsecond)
				}
				read = atomic.LoadUint64(&q.critRead)
				if alloc-read < fraudCriticalUsable {
					goto spaceOK
				}
			}
			metrics.FraudStreamCriticalDropTotal.Inc()
			return false
		}
	spaceOK:
		if !atomic.CompareAndSwapUint64(&q.critAlloc, alloc, alloc+1) {
			continue
		}
		idx := alloc & fraudCriticalMask
		fillFraudSlot(&q.critSlots[idx], shard, evt)
		for {
			if atomic.LoadUint64(&q.critWrite) == alloc {
				atomic.StoreUint64(&q.critWrite, alloc+1)
				return true
			}
			runtime.Gosched()
		}
	}
}

func (q *FraudStreamWriter) enqueueAnalytical(shard int, evt *campaignmodel.Event) bool {
	return q.enqueueRing(shard, evt)
}

// enqueueRing copies a fraud event into the analytical MPSC ring; false means overflow.
func (q *FraudStreamWriter) enqueueRing(shard int, evt *campaignmodel.Event) bool {
	for {
		alloc := atomic.LoadUint64(&q.allocCursor)
		read := atomic.LoadUint64(&q.readCursor)
		if alloc-read >= fraudAnalyticalUsable {
			for spin := 0; spin < 100; spin++ {
				if spin < 20 {
					runtime.Gosched()
				} else {
					time.Sleep(time.Microsecond)
				}
				read = atomic.LoadUint64(&q.readCursor)
				if alloc-read < fraudAnalyticalUsable {
					goto spaceAvailable
				}
			}
			return false
		}
		if alloc-read >= fraudAnalyticalUsable-512 {
			runtime.Gosched()
		}
	spaceAvailable:
		if !atomic.CompareAndSwapUint64(&q.allocCursor, alloc, alloc+1) {
			continue
		}

		idx := alloc % fraudAnalyticalCapacity
		fillFraudSlot(&q.slots[idx], shard, evt)

		for {
			if atomic.LoadUint64(&q.writeCursor) == alloc {
				atomic.StoreUint64(&q.writeCursor, alloc+1)
				return true
			}
			runtime.Gosched()
		}
	}
}

// Pending exposes ring backlog so operators can alert before fraud telemetry is dropped.
func (q *FraudStreamWriter) Pending() uint64 {
	if q == nil {
		return 0
	}
	return pendingDelta(atomic.LoadUint64(&q.writeCursor), atomic.LoadUint64(&q.readCursor)) +
		pendingDelta(atomic.LoadUint64(&q.critWrite), atomic.LoadUint64(&q.critRead))
}

func pendingDelta(head, tail uint64) uint64 {
	if head <= tail {
		return 0
	}
	return head - tail
}

// Stop drains pending fraud events and waits for the background worker to exit.
func (q *FraudStreamWriter) Stop() {
	if q == nil {
		return
	}
	select {
	case <-q.stopCh:
		return
	default:
		close(q.stopCh)
	}
	q.wg.Wait()
	q.aggWg.Wait()
}

func (q *FraudStreamWriter) worker() {
	defer q.wg.Done()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-q.stopCh:
			q.drain(true)
			return
		case <-ticker.C:
			q.drain(false)
		}
	}
}

func (q *FraudStreamWriter) drain(final bool) {
	ctx := context.Background()
	batch := make([]*fraudStreamSlot, 0, fraudFlushBatch)

	drainLane := func(writePtr, readPtr *uint64, slots []fraudStreamSlot, capacity uint64) {
		for {
			writeCursor := atomic.LoadUint64(writePtr)
			readCursor := atomic.LoadUint64(readPtr)
			if readCursor >= writeCursor {
				break
			}
			if len(batch) >= fraudFlushBatch {
				q.flushBatch(ctx, batch)
				batch = batch[:0]
			}
			idx := readCursor % capacity
			slot := &slots[idx]
			for slot.ready.Load() == 0 {
				runtime.Gosched()
			}
			batch = append(batch, slot)
			atomic.StoreUint64(readPtr, readCursor+1)
		}
	}

	drainLane(&q.critWrite, &q.critRead, q.critSlots[:], fraudCriticalCapacity)
	drainLane(&q.writeCursor, &q.readCursor, q.slots[:], fraudAnalyticalCapacity)

	if len(batch) > 0 {
		q.flushBatch(ctx, batch)
	}

	if final {
		for atomic.LoadUint64(&q.writeCursor) != atomic.LoadUint64(&q.readCursor) ||
			atomic.LoadUint64(&q.critWrite) != atomic.LoadUint64(&q.critRead) {
			runtime.Gosched()
		}
	}
	q.publishRingGauges(q.ringFill())
}

func (q *FraudStreamWriter) flushBatch(ctx context.Context, batch []*fraudStreamSlot) {
	if len(batch) == 0 {
		return
	}

	type shardBatch struct {
		pipe redis.Pipeliner
		cmds []*redis.StringCmd
	}
	shards := make(map[uint8]*shardBatch)
	wraps := make([]*ByteSliceValue, 0, len(batch))
	bufs := make([]*[]byte, 0, len(batch))
	values := make([][]any, 0, len(batch))

	for _, slot := range batch {
		data, wrap, bufPtr := marshalFraudStreamSlot(slot)
		if data == nil {
			filterFraudStreamWriteErrors.Inc()
			continue
		}
		wraps = append(wraps, wrap)
		bufs = append(bufs, bufPtr)
		vals := []any{"d", wrap}
		values = append(values, vals)

		shard := slot.shard
		sb, ok := shards[shard]
		if !ok {
			sb = &shardBatch{pipe: q.rdbs[shard].Pipeline()}
			shards[shard] = sb
		}
		cmd := sb.pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: q.stream,
			MaxLen: q.maxLen,
			Approx: true,
			Values: vals,
		})
		sb.cmds = append(sb.cmds, cmd)
	}

	for _, sb := range shards {
		_, _ = sb.pipe.Exec(ctx)
		for _, cmd := range sb.cmds {
			if cmd.Err() != nil {
				filterFraudStreamWriteErrors.Inc()
			}
		}
	}

	for i := range wraps {
		byteSliceValuePool.Put(wraps[i])
		byteBufPool.Put(bufs[i])
	}
	_ = values
}

func marshalFraudStreamSlot(slot *fraudStreamSlot) ([]byte, *ByteSliceValue, *[]byte) {
	pbEvt := streamEventPool.Get().(*pb.AdStreamEvent)
	DeepResetAdStreamEvent(pbEvt)
	pbEvt.ClickId = slot.clickID[:slot.clickLen]
	pbEvt.CampaignId = slot.campaignID[:]
	pbEvt.EventType = slot.evtType[:slot.typeLen]
	pbEvt.Payload = slot.payload[:slot.payloadLen]
	pbEvt.Ip = slot.ip[:slot.ipLen]
	pbEvt.Ua = slot.ua[:slot.uaLen]
	pbEvt.UserId = slot.userID[:slot.userLen]
	pbEvt.CreatedAtUnix = 0
	if slot.createdAt > 0 {
		pbEvt.CreatedAtUnix = slot.createdAt / int64(time.Second)
	}
	pbEvt.FraudScore = slot.fraudScore
	pbEvt.FraudReason = slot.reason[:slot.reasonLen]
	pbEvt.GhostEvent = slot.ghostEvent

	size := pbEvt.SizeVT()
	bufPtr := byteBufPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	n, err := pbEvt.MarshalToSizedBufferVT(buf)
	ClearAdStreamEvent(pbEvt)
	streamEventPool.Put(pbEvt)
	if err != nil {
		*bufPtr = buf
		byteBufPool.Put(bufPtr)
		return nil, nil, nil
	}
	data := buf[:n]
	*bufPtr = buf
	wrap := byteSliceValuePool.Get().(*ByteSliceValue)
	wrap.b = data
	return data, wrap, bufPtr
}

// enqueueFraudReject enqueues a rejected fraud event, counting analytical drops when the ring is full.
// Critical-lane drops are counted inside enqueueCritical.
func enqueueFraudReject(writer *FraudStreamWriter, shard int, evt *campaignmodel.Event) {
	if writer == nil {
		return
	}
	if !writer.Enqueue(shard, evt) {
		if !fraudAggregateExempt(evt) {
			metrics.FraudStreamDropTotal.Inc()
		}
	}
}

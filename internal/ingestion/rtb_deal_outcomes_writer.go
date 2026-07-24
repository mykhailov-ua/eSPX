package ingestion

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/rtb"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	rtbOutcomeRingCapacity = 4096
	rtbOutcomeRingMask     = rtbOutcomeRingCapacity - 1
	rtbOutcomeRingUsable   = rtbOutcomeRingCapacity - 1
	rtbOutcomeFlushBatch   = 128
	rtbOutcomeDealIDMax    = 64
	rtbOutcomeFlushEvery   = 5 * time.Second
)

type rtbOutcomeSlot struct {
	ready      atomic.Uint32
	dealLen    uint8
	outcome    uint8
	_          [2]byte
	floorMicro int64
	createdAt  int64
	dealID     [rtbOutcomeDealIDMax]byte
}

// rtbOutcomeRow is a cold-path snapshot drained from rtbOutcomeSlot (no atomics — safe to copy).
type rtbOutcomeRow struct {
	dealLen    uint8
	outcome    uint8
	_          [2]byte
	floorMicro int64
	createdAt  int64
	dealID     [rtbOutcomeDealIDMax]byte
}

// RtbDealOutcomeWriter batches PMP auction outcomes to ClickHouse on a lossy ring.
type RtbDealOutcomeWriter struct {
	_           [64]byte
	writeCursor uint64
	_           [64]byte
	allocCursor uint64
	_           [64]byte
	readCursor  uint64
	_           [64]byte
	slots       [rtbOutcomeRingCapacity]rtbOutcomeSlot

	conn driver.Conn

	stopCh chan struct{}
	wg     sync.WaitGroup
}

var globalRtbOutcomeWriter atomic.Pointer[RtbDealOutcomeWriter]

// SetRtbDealOutcomeWriter installs the process-wide outcome writer (tracker startup).
func SetRtbDealOutcomeWriter(w *RtbDealOutcomeWriter) {
	globalRtbOutcomeWriter.Store(w)
}

// NewRtbDealOutcomeWriter starts the background drainer when ClickHouse is configured.
func NewRtbDealOutcomeWriter(conn driver.Conn) *RtbDealOutcomeWriter {
	if conn == nil {
		return nil
	}
	w := &RtbDealOutcomeWriter{
		conn:   conn,
		stopCh: make(chan struct{}),
	}
	w.wg.Add(1)
	go w.worker()
	return w
}

// Close stops the background worker.
func (w *RtbDealOutcomeWriter) Close() {
	if w == nil {
		return
	}
	close(w.stopCh)
	w.wg.Wait()
}

// Enqueue records one deal outcome; false means the ring overflowed and the row is dropped.
func (w *RtbDealOutcomeWriter) Enqueue(dealID []byte, outcome uint8, floorMicro int64) bool {
	if w == nil {
		return true
	}
	for {
		alloc := atomic.LoadUint64(&w.allocCursor)
		read := atomic.LoadUint64(&w.readCursor)
		if alloc-read >= rtbOutcomeRingUsable {
			return false
		}
		if !atomic.CompareAndSwapUint64(&w.allocCursor, alloc, alloc+1) {
			continue
		}
		idx := alloc & rtbOutcomeRingMask
		slot := &w.slots[idx]
		for slot.ready.Load() != 0 {
			// slot not drained yet — treat as lossy drop
			return false
		}
		ln := len(dealID)
		if ln > rtbOutcomeDealIDMax {
			ln = rtbOutcomeDealIDMax
		}
		slot.dealLen = uint8(ln)
		slot.outcome = outcome
		slot.floorMicro = floorMicro
		slot.createdAt = time.Now().UTC().UnixMilli()
		for i := 0; i < ln; i++ {
			slot.dealID[i] = dealID[i]
		}
		slot.ready.Store(1)
		atomic.StoreUint64(&w.writeCursor, alloc+1)
		return true
	}
}

func recordRtbDealOutcome(dealID string, floorMicro int64, res rtb.AuctionResult, reason rtb.NoBidReason) {
	if dealID == "" {
		recordRtbDealOutcomeBytes(nil, 0, floorMicro, res, reason)
		return
	}
	var buf [rtbOutcomeDealIDMax]byte
	n := copy(buf[:], dealID)
	recordRtbDealOutcomeBytes(buf[:n], uint8(n), floorMicro, res, reason)
}

func recordRtbDealOutcomeBytes(dealID []byte, dealLen uint8, floorMicro int64, res rtb.AuctionResult, reason rtb.NoBidReason) {
	w := globalRtbOutcomeWriter.Load()
	if w == nil {
		return
	}
	outcome := uint8(0)
	if reason.OK() {
		outcome = 1
	}
	var buf []byte
	if dealLen > 0 {
		buf = dealID[:dealLen]
	}
	_ = w.Enqueue(buf, outcome, floorMicro)
}

func (w *RtbDealOutcomeWriter) worker() {
	defer w.wg.Done()
	ticker := time.NewTicker(rtbOutcomeFlushEvery)
	defer ticker.Stop()
	batch := make([]rtbOutcomeRow, 0, rtbOutcomeFlushBatch)
	for {
		select {
		case <-w.stopCh:
			w.drainBatch(&batch)
			return
		case <-ticker.C:
			w.drainBatch(&batch)
		default:
			if w.collectBatch(&batch) {
				w.flushBatch(batch)
				batch = batch[:0]
			} else {
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (w *RtbDealOutcomeWriter) collectBatch(batch *[]rtbOutcomeRow) bool {
	read := atomic.LoadUint64(&w.readCursor)
	write := atomic.LoadUint64(&w.writeCursor)
	if read >= write {
		return false
	}
	for read < write && len(*batch) < rtbOutcomeFlushBatch {
		idx := read & rtbOutcomeRingMask
		slot := &w.slots[idx]
		if slot.ready.Load() == 0 {
			break
		}
		*batch = append(*batch, rtbOutcomeRow{
			dealLen:    slot.dealLen,
			outcome:    slot.outcome,
			floorMicro: slot.floorMicro,
			createdAt:  slot.createdAt,
			dealID:     slot.dealID,
		})
		slot.ready.Store(0)
		read++
	}
	atomic.StoreUint64(&w.readCursor, read)
	return len(*batch) > 0
}

func (w *RtbDealOutcomeWriter) drainBatch(batch *[]rtbOutcomeRow) {
	for w.collectBatch(batch) {
		w.flushBatch(*batch)
		*batch = (*batch)[:0]
	}
}

func (w *RtbDealOutcomeWriter) flushBatch(batch []rtbOutcomeRow) {
	if len(batch) == 0 || w.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	chBatch, err := w.conn.PrepareBatch(ctx, `INSERT INTO rtb_deal_outcomes (deal_id, outcome, floor_micro, created_at)`)
	if err != nil {
		return
	}
	for i := range batch {
		slot := &batch[i]
		dealID := string(slot.dealID[:slot.dealLen])
		created := time.UnixMilli(slot.createdAt).UTC()
		if err := chBatch.Append(dealID, slot.outcome, slot.floorMicro, created); err != nil {
			_ = chBatch.Abort()
			return
		}
	}
	_ = chBatch.Send()
}

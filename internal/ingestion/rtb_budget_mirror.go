package ingestion

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/rtb"

	redis "github.com/redis/go-redis/v9"
)

const (
	rtbBudgetMirrorRingCapacity = 4096
	rtbBudgetMirrorRingMask     = rtbBudgetMirrorRingCapacity - 1
	rtbBudgetMirrorRingUsable   = rtbBudgetMirrorRingCapacity - 1
	rtbBudgetMirrorFlushBatch   = 128
	rtbBudgetMirrorFlushEvery   = 2 * time.Second
)

type rtbBudgetMirrorSlot struct {
	ready      atomic.Uint32
	priceMicro int64
	campaignID rtb.CampaignID
}

// RtbBudgetMirrorWriter async-flushes RTB authoritative debits to Redis (R20).
type RtbBudgetMirrorWriter struct {
	_           [64]byte
	writeCursor uint64
	_           [64]byte
	allocCursor uint64
	_           [64]byte
	readCursor  uint64
	_           [64]byte
	slots       [rtbBudgetMirrorRingCapacity]rtbBudgetMirrorSlot

	catalog  *RtbCatalog
	registry *Registry
	rdbs     []redis.UniversalClient
	sharder  Sharder

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewRtbBudgetMirrorWriter starts the background Redis mirror drainer when authority=rtb.
func NewRtbBudgetMirrorWriter(catalog *RtbCatalog, registry *Registry, rdbs []redis.UniversalClient, sharder Sharder) *RtbBudgetMirrorWriter {
	if catalog == nil || registry == nil || len(rdbs) == 0 || sharder == nil {
		return nil
	}
	w := &RtbBudgetMirrorWriter{
		catalog:  catalog,
		registry: registry,
		rdbs:     rdbs,
		sharder:  sharder,
		stopCh:   make(chan struct{}),
	}
	w.wg.Add(1)
	go w.worker()
	rtb.SetBudgetSpendMirror(w)
	return w
}

// RecordSpend enqueues one authoritative RTB debit for async Redis mirror.
func (w *RtbBudgetMirrorWriter) RecordSpend(campaignID rtb.CampaignID, _ uint32, priceMicro int64) {
	if w == nil || priceMicro <= 0 {
		return
	}
	for {
		alloc := atomic.LoadUint64(&w.allocCursor)
		read := atomic.LoadUint64(&w.readCursor)
		if alloc-read >= rtbBudgetMirrorRingUsable {
			return
		}
		if !atomic.CompareAndSwapUint64(&w.allocCursor, alloc, alloc+1) {
			continue
		}
		idx := alloc & rtbBudgetMirrorRingMask
		slot := &w.slots[idx]
		if slot.ready.Load() != 0 {
			return
		}
		slot.campaignID = campaignID
		slot.priceMicro = priceMicro
		slot.ready.Store(1)
		atomic.StoreUint64(&w.writeCursor, alloc+1)
		return
	}
}

// Close stops the drainer and clears the global mirror hook.
func (w *RtbBudgetMirrorWriter) Close() {
	if w == nil {
		return
	}
	rtb.SetBudgetSpendMirror(nil)
	close(w.stopCh)
	w.wg.Wait()
}

func (w *RtbBudgetMirrorWriter) worker() {
	defer w.wg.Done()
	ticker := time.NewTicker(rtbBudgetMirrorFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			w.flush(context.Background())
			return
		case <-ticker.C:
			w.flush(context.Background())
		}
	}
}

func (w *RtbBudgetMirrorWriter) flush(ctx context.Context) {
	batch := 0
	for batch < rtbBudgetMirrorFlushBatch {
		read := atomic.LoadUint64(&w.readCursor)
		write := atomic.LoadUint64(&w.writeCursor)
		if read >= write {
			return
		}
		idx := read & rtbBudgetMirrorRingMask
		slot := &w.slots[idx]
		if slot.ready.Load() != 1 {
			return
		}
		w.applyDebit(ctx, slot.campaignID, slot.priceMicro)
		slot.ready.Store(0)
		atomic.StoreUint64(&w.readCursor, read+1)
		batch++
	}
}

func (w *RtbBudgetMirrorWriter) applyDebit(ctx context.Context, campID rtb.CampaignID, priceMicro int64) {
	uid, ok := w.catalog.UUIDForWinner(campID)
	if !ok {
		return
	}
	camp, ok := w.registry.GetCampaign(uid)
	if !ok || camp == nil || camp.BudgetCampaignKey == "" {
		return
	}
	shard := w.sharder.GetShard(uid)
	if shard < 0 || shard >= len(w.rdbs) {
		return
	}
	rdb := w.rdbs[shard]
	if rdb == nil {
		return
	}
	_ = rdb.DecrBy(ctx, camp.BudgetCampaignKey, priceMicro).Err()
}

var _ rtb.BudgetSpendMirror = (*RtbBudgetMirrorWriter)(nil)

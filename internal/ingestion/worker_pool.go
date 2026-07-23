package ingestion

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// finishOffloadCtx runs test hooks when no handler is bound or the job is a noop enqueue.
func finishOffloadCtx(ctx *connContext) {
	if ctx == nil {
		return
	}
	if ctx.offloadOnEnter != nil {
		ctx.offloadOnEnter()
	}
	if ctx.offloadBlock != nil {
		<-ctx.offloadBlock
	}
	if ctx.offloadWG != nil {
		ctx.offloadWG.Done()
	}
}

type Slot struct {
	ctx   *connContext
	ready atomic.Bool
}

// MPSCQueue isolates producer and consumer cache lines to reduce false sharing under load.
type MPSCQueue struct {
	_     [8]uint64
	write uint64
	_     [8]uint64
	read  uint64
	_     [8]uint64
	mask  uint64
	ring  []Slot
}

func NewMPSCQueue(size uint64) *MPSCQueue {
	if size == 0 || (size&(size-1)) != 0 {
		size = 4096
	}
	return &MPSCQueue{
		mask: size - 1,
		ring: make([]Slot, size),
	}
}

// PushCtx enqueues an offload context from any producer goroutine; returns false when full.
func (q *MPSCQueue) PushCtx(ctx *connContext) bool {
	for {
		w := atomic.LoadUint64(&q.write)
		r := atomic.LoadUint64(&q.read)
		if w-r >= q.mask+1 {
			return false
		}
		if atomic.CompareAndSwapUint64(&q.write, w, w+1) {
			slot := &q.ring[w&q.mask]
			slot.ctx = ctx
			slot.ready.Store(true)
			return true
		}
	}
}

func (q *MPSCQueue) PopCtx() (*connContext, bool) {
	r := atomic.LoadUint64(&q.read)
	w := atomic.LoadUint64(&q.write)
	if r == w {
		return nil, false
	}
	slot := &q.ring[r&q.mask]
	if !slot.ready.Load() {
		return nil, false
	}
	ctx := slot.ctx
	slot.ctx = nil
	slot.ready.Store(false)
	atomic.StoreUint64(&q.read, r+1)
	return ctx, true
}

// Worker runs tasks on a dedicated OS thread for predictable gnet offload latency.
type Worker struct {
	pool  *PinnedWorkerPool
	id    int
	queue *MPSCQueue
}

// start loops forever dequeuing and executing tasks on a locked OS thread.
func (w *Worker) start() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	run := func(ctx *connContext) {
		if h := w.pool.handler; h != nil {
			h.runOffloadedRequest(w.id, ctx)
		} else {
			finishOffloadCtx(ctx)
		}
		w.pool.wg.Done()
	}

	for {
		ctx, ok := w.queue.PopCtx()
		if ok {
			run(ctx)
			continue
		}

		if atomic.LoadInt32(&w.pool.closed) == 1 {
			if ctx, ok = w.queue.PopCtx(); !ok {
				break
			}
			run(ctx)
			continue
		}

		spin := 0
		for {
			if ctx, ok = w.queue.PopCtx(); ok {
				run(ctx)
				break
			}
			if atomic.LoadInt32(&w.pool.closed) == 1 {
				break
			}
			if spin < 10 {
				spin++
			} else if spin < 20 {
				spin++
				runtime.Gosched()
			} else {
				time.Sleep(time.Microsecond)
			}
		}
	}
}

// PinnedWorkerPool offloads gnet React work to pinned threads to keep the event loop responsive.
type PinnedWorkerPool struct {
	workers []*Worker
	handler *AdsPacketHandler
	round   uint64
	wg      sync.WaitGroup
	closed  int32
}

func NewPinnedWorkerPool(size int, queueSize int) *PinnedWorkerPool {
	if size <= 0 {
		size = runtime.GOMAXPROCS(0)
	}
	qSize := uint64(queueSize)
	if qSize == 0 || (qSize&(qSize-1)) != 0 {
		qSize = 4096
	}

	p := &PinnedWorkerPool{
		workers: make([]*Worker, size),
	}
	for i := 0; i < size; i++ {
		w := &Worker{
			pool:  p,
			id:    i,
			queue: NewMPSCQueue(qSize),
		}
		p.workers[i] = w
		go w.start()
	}
	return p
}

// SubmitOffload schedules ctx for pinned-thread processing; returns false when all queues are saturated.
func (p *PinnedWorkerPool) SubmitOffload(ctx *connContext) bool {
	if atomic.LoadInt32(&p.closed) == 1 {
		return false
	}
	p.wg.Add(1)

	idx := atomic.AddUint64(&p.round, 1) % uint64(len(p.workers))
	if p.workers[idx].queue.PushCtx(ctx) {
		return true
	}

	for i := 1; i < len(p.workers); i++ {
		nextIdx := (idx + uint64(i)) % uint64(len(p.workers))
		if p.workers[nextIdx].queue.PushCtx(ctx) {
			return true
		}
	}

	p.wg.Done()
	return false
}

// Shutdown closes the pool and waits for in-flight tasks to finish.
func (p *PinnedWorkerPool) Shutdown() {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		p.wg.Wait()
	}
}

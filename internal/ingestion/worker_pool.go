package ingestion

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Slot holds one queued task in the MPSC ring buffer.
type Slot struct {
	task  func()
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

// Push enqueues a task from any producer goroutine; returns false when the ring is full.
func (q *MPSCQueue) Push(fn func()) bool {
	for {
		w := atomic.LoadUint64(&q.write)
		r := atomic.LoadUint64(&q.read)
		if w-r >= q.mask+1 {
			return false
		}
		if atomic.CompareAndSwapUint64(&q.write, w, w+1) {
			slot := &q.ring[w&q.mask]
			slot.task = fn
			slot.ready.Store(true)
			return true
		}
	}
}

func (q *MPSCQueue) Pop() (func(), bool) {
	r := atomic.LoadUint64(&q.read)
	w := atomic.LoadUint64(&q.write)
	if r == w {
		return nil, false
	}
	slot := &q.ring[r&q.mask]
	if !slot.ready.Load() {
		return nil, false
	}
	fn := slot.task
	slot.task = nil
	slot.ready.Store(false)
	atomic.StoreUint64(&q.read, r+1)
	return fn, true
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

	for {
		fn, ok := w.queue.Pop()
		if ok {
			fn()
			w.pool.wg.Done()
			continue
		}

		if atomic.LoadInt32(&w.pool.closed) == 1 {
			if _, ok := w.queue.Pop(); !ok {
				break
			}
		}

		spin := 0
		for {
			if fn, ok = w.queue.Pop(); ok {
				fn()
				w.pool.wg.Done()
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

// bindWorkerTask captures workerID by value for MPSC queue closures.
func bindWorkerTask(workerID int, fn func(int)) func() {
	return func() { fn(workerID) }
}

// Submit schedules fn on the next worker; returns false when all queues are saturated.
func (p *PinnedWorkerPool) Submit(fn func(workerID int)) bool {
	if atomic.LoadInt32(&p.closed) == 1 {
		return false
	}
	p.wg.Add(1)

	idx := atomic.AddUint64(&p.round, 1) % uint64(len(p.workers))
	if p.workers[idx].queue.Push(bindWorkerTask(p.workers[idx].id, fn)) {
		return true
	}

	for i := 1; i < len(p.workers); i++ {
		nextIdx := (idx + uint64(i)) % uint64(len(p.workers))
		w := p.workers[nextIdx]
		if w.queue.Push(bindWorkerTask(w.id, fn)) {
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

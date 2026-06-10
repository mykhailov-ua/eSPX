package ads

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Slot struct {
	task  func()
	ready atomic.Bool
}

// MPSCQueue is a lock-free MPSC ring buffer with cache-line padding between
// producer write and consumer read indices to avoid false sharing.
type MPSCQueue struct {
	_     [8]uint64 // cache line padding (64 bytes)
	write uint64
	_     [8]uint64 // cache line padding (64 bytes)
	read  uint64
	_     [8]uint64 // cache line padding (64 bytes)
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

type Worker struct {
	pool  *PinnedWorkerPool
	id    int
	queue *MPSCQueue
}

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

func (p *PinnedWorkerPool) Submit(fn func()) bool {
	if atomic.LoadInt32(&p.closed) == 1 {
		return false
	}
	p.wg.Add(1)

	idx := atomic.AddUint64(&p.round, 1) % uint64(len(p.workers))
	if p.workers[idx].queue.Push(fn) {
		return true
	}

	for i := 1; i < len(p.workers); i++ {
		nextIdx := (idx + uint64(i)) % uint64(len(p.workers))
		if p.workers[nextIdx].queue.Push(fn) {
			return true
		}
	}

	p.wg.Done()
	return false
}

func (p *PinnedWorkerPool) Shutdown() {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		p.wg.Wait()
	}
}

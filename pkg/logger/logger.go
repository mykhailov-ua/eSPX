package logger

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type LogPayload struct {
	ready    atomic.Uint32
	Priority uint8
	Length   uint32
	Data     [500]byte
}

type LogShard struct {
	_           [64]byte
	writeCursor uint64 // published tail (visible to drainer)
	_           [64]byte
	allocCursor uint64 // reserved tail (producers CAS)
	_           [64]byte
	readCursor  uint64
	_           [64]byte
	slots       [65536]LogPayload
}

const (
	RingCapacity = 65536
	RingMask     = RingCapacity - 1
	ringUsable   = RingCapacity - 1
)

func NewLogShard() *LogShard {
	return &LogShard{}
}

func (s *LogShard) Write(priority uint8, data []byte) bool {
	for {
		alloc := atomic.LoadUint64(&s.allocCursor)
		read := atomic.LoadUint64(&s.readCursor)
		if alloc-read >= ringUsable {
			return false
		}
		if !atomic.CompareAndSwapUint64(&s.allocCursor, alloc, alloc+1) {
			continue
		}

		idx := alloc & RingMask
		payload := &s.slots[idx]
		payload.ready.Store(0)
		payload.Priority = priority
		payload.Length = uint32(copy(payload.Data[:], data))
		payload.ready.Store(1)

		for {
			pub := atomic.LoadUint64(&s.writeCursor)
			if pub == alloc {
				if atomic.CompareAndSwapUint64(&s.writeCursor, pub, pub+1) {
					return true
				}
				continue
			}
			runtime.Gosched()
		}
	}
}

type Config struct {
	LogDir                string
	FlushBufferSize       int
	RotateSize            int64
	RotateInterval        time.Duration
	DiskLatencyLimit      time.Duration
	PersistQueueDepth     int
	PersistEnqueueTimeout time.Duration
}

const (
	defaultAvgLogLineBytes   = 200
	minPersistQueueDepth     = 64
	maxPersistQueueDepth     = 4096
	defaultPersistEnqueueDur = 25 * time.Millisecond
)

func ComputePersistQueueDepth(cfg Config) int {
	if cfg.PersistQueueDepth > 0 {
		if cfg.PersistQueueDepth > maxPersistQueueDepth {
			return maxPersistQueueDepth
		}
		return cfg.PersistQueueDepth
	}
	flush := cfg.FlushBufferSize
	if flush <= 0 {
		flush = 256 * 1024
	}
	depth := (2 * flush / defaultAvgLogLineBytes) * 2
	if depth < minPersistQueueDepth {
		depth = minPersistQueueDepth
	}
	if depth > maxPersistQueueDepth {
		depth = maxPersistQueueDepth
	}
	return depth
}

type Logger struct {
	cfg                   Config
	shards                []*LogShard
	activeFile            *os.File
	fileOpenedAt          time.Time
	bytesWritten          int64
	diskDegraded          atomic.Int32
	loadSheddingEvents    atomic.Uint64
	persistQueueDrops     atomic.Uint64
	persistQueueDropBytes atomic.Uint64
	emaLatency            atomic.Uint64
	writerIndex           atomic.Uint64
	persistCh             chan *AlignedBuffer
	persistQueueCap       int
	wg                    sync.WaitGroup
	closeChan             chan struct{}
}

func NewLogger(cfg Config, numShards int) *Logger {
	if cfg.PersistEnqueueTimeout <= 0 {
		cfg.PersistEnqueueTimeout = defaultPersistEnqueueDur
	}
	queueDepth := ComputePersistQueueDepth(cfg)
	shards := make([]*LogShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = NewLogShard()
	}
	l := &Logger{
		cfg:             cfg,
		shards:          shards,
		persistCh:       make(chan *AlignedBuffer, queueDepth),
		persistQueueCap: queueDepth,
		closeChan:       make(chan struct{}),
	}
	_ = os.MkdirAll(l.cfg.LogDir, 0755)
	l.openActiveFile()
	l.wg.Add(3)
	go l.StartDrainer()
	go l.StartPersister()
	go l.StartDiskMonitor()
	return l
}

func (l *Logger) Close() {
	close(l.closeChan)
	l.wg.Wait()
	if l.activeFile != nil {
		_ = l.activeFile.Close()
	}
}

func (l *Logger) Shards() []*LogShard {
	return l.shards
}

func (l *Logger) WriteToShard(shardID int, priority uint8, data []byte) bool {
	if shardID < 0 || shardID >= len(l.shards) {
		return false
	}
	return l.shards[shardID].Write(priority, data)
}

func (l *Logger) Write(priority uint8, data []byte) bool {
	shardID := int(l.writerIndex.Add(1) % uint64(len(l.shards)))
	return l.shards[shardID].Write(priority, data)
}

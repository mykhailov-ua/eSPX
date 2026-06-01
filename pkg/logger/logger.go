package logger

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type LogPayload struct {
	Data     [512]byte
	Length   uint32
	Priority uint8
}

type LogShard struct {
	_           [64]byte
	writeCursor uint64
	_           [64]byte
	readCursor  uint64
	_           [64]byte
	slots       [65536]LogPayload
}

const (
	RingCapacity = 65536
	RingMask     = 65535
)

func NewLogShard() *LogShard {
	return &LogShard{}
}

func (s *LogShard) Write(priority uint8, data []byte) bool {
	writeCursor := atomic.LoadUint64(&s.writeCursor)
	readCursor := atomic.LoadUint64(&s.readCursor)
	if writeCursor-readCursor >= RingCapacity {
		return false
	}
	idx := writeCursor & RingMask
	s.slots[idx].Priority = priority
	s.slots[idx].Length = uint32(copy(s.slots[idx].Data[:], data))
	atomic.StoreUint64(&s.writeCursor, writeCursor+1)
	return true
}

type Config struct {
	LogDir           string
	FlushBufferSize  int
	RotateSize       int64
	RotateInterval   time.Duration
	DiskLatencyLimit time.Duration
}

type Logger struct {
	cfg                Config
	shards             []*LogShard
	activeFile         *os.File
	fileOpenedAt       time.Time
	bytesWritten       int64
	diskDegraded       atomic.Int32
	loadSheddingEvents atomic.Uint64
	emaLatency         atomic.Uint64
	persistCh          chan *AlignedBuffer
	wg                 sync.WaitGroup
	closeChan          chan struct{}
}

func NewLogger(cfg Config, numShards int) *Logger {
	shards := make([]*LogShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = NewLogShard()
	}
	l := &Logger{
		cfg:       cfg,
		shards:    shards,
		persistCh: make(chan *AlignedBuffer, 2),
		closeChan: make(chan struct{}),
	}
	l.openActiveFile()
	l.wg.Add(2)
	go l.StartDrainer()
	go l.StartPersister()
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

var (
	globalWriterIndex uint64
	globalMu          sync.Mutex
)

func (l *Logger) Write(priority uint8, data []byte) bool {
	globalMu.Lock()
	defer globalMu.Unlock()
	shardID := int(atomic.AddUint64(&globalWriterIndex, 1) % uint64(len(l.shards)))
	return l.shards[shardID].Write(priority, data)
}

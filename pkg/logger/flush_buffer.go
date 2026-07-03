package logger

import (
	"sync"
	"unsafe"
)

// AlignedBuffer batches framed log records on a page-aligned slice so NVMe writes avoid extra memcpy and unaligned syscall buffers.
type AlignedBuffer struct {
	raw     []byte
	aligned []byte
	offset  int
}

// NewAlignedBuffer allocates slack past the target size so the writable slice starts on a 4KiB boundary.
func NewAlignedBuffer(size int) *AlignedBuffer {
	raw := make([]byte, size+4096)
	ptr := uintptr(unsafe.Pointer(&raw[0]))
	misalignment := ptr & 4095
	var offset uintptr
	if misalignment != 0 {
		offset = 4096 - misalignment
	}
	aligned := raw[offset : offset+uintptr(size)]
	return &AlignedBuffer{
		raw:     raw,
		aligned: aligned,
		offset:  0,
	}
}

// Write appends raw bytes into the current batch without allocating per record.
func (b *AlignedBuffer) Write(data []byte) int {
	n := copy(b.aligned[b.offset:], data)
	b.offset += n
	return n
}

// WriteByte appends one framed length or delimiter byte into the batch.
func (b *AlignedBuffer) WriteByte(c byte) error {
	b.aligned[b.offset] = c
	b.offset++
	return nil
}

// Reset clears the batch so a pooled buffer can be reused for the next drain cycle.
func (b *AlignedBuffer) Reset() {
	b.offset = 0
}

// Bytes returns the filled prefix of the batch ready for a single disk write.
func (b *AlignedBuffer) Bytes() []byte {
	return b.aligned[:b.offset]
}

// Available reports remaining capacity before the drainer must flush or grow the batch.
func (b *AlignedBuffer) Available() int {
	return len(b.aligned) - b.offset
}

// bufferPool recycles aligned batches so the drainer path does not allocate on every tick.
var bufferPool sync.Pool

// getBuffer returns a pooled batch sized for the configured flush threshold.
func (l *Logger) getBuffer() *AlignedBuffer {
	val := bufferPool.Get()
	if val == nil {
		return NewAlignedBuffer(l.cfg.FlushBufferSize)
	}
	buf := val.(*AlignedBuffer)
	if len(buf.aligned) < l.cfg.FlushBufferSize {
		return NewAlignedBuffer(l.cfg.FlushBufferSize)
	}
	return buf
}

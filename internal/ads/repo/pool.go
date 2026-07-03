package repo

import (
	"sync"

	"espx/internal/ads/pb"
)

// StreamEventPool recycles protobuf stream events to avoid allocations on produce/consume paths.
var StreamEventPool = sync.Pool{
	New: func() any {
		return new(pb.AdStreamEvent)
	},
}

// AdLogRecordPool recycles protobuf audit records written after successful stores.
var AdLogRecordPool = sync.Pool{
	New: func() any {
		return &pb.AdLogRecord{}
	},
}

// ByteBufPool recycles marshal buffers for stream payloads.
var ByteBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// DLQEventPool recycles DLQ protobuf payloads before writing to the dead letter stream.
var DLQEventPool = sync.Pool{
	New: func() any {
		return &pb.AdDLQEvent{}
	},
}

// ByteSliceValuePool recycles Redis binary marshaling wrappers.
var ByteSliceValuePool = sync.Pool{
	New: func() any {
		return new(ByteSliceValue)
	},
}

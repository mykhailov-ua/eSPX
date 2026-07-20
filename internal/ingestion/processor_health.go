package ingestion

import "sync/atomic"

// ProcessorHealthState holds cached processor readiness inputs updated off the hot path.
var ProcessorHealthState struct {
	streamLagSec atomic.Int64
}

// SetProcessorStreamLagSec records stream lag for /readyz gating.
func SetProcessorStreamLagSec(sec int64) {
	if sec < 0 {
		sec = 0
	}
	ProcessorHealthState.streamLagSec.Store(sec)
}

// ProcessorStreamLagSec returns the last observed main-stream lag in seconds.
func ProcessorStreamLagSec() int64 {
	return ProcessorHealthState.streamLagSec.Load()
}

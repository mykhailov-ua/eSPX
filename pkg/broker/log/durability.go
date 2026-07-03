// Package log durability modes control when mmap writes become durable on disk.
package log

import (
	"fmt"
	"time"
)

// DurabilityMode selects the fsync policy for leader appends.
type DurabilityMode int

const (
	// DurabilityAsync fsyncs on a background ticker only (default, lowest latency).
	DurabilityAsync DurabilityMode = iota
	// DurabilityGroupCommit fsyncs after N records or on the flush interval, whichever comes first.
	DurabilityGroupCommit
	// DurabilitySync fsyncs before returning from each leader append (strongest RPO).
	DurabilitySync
)

// DurabilityConfig tunes flush timing and group-commit batching.
type DurabilityConfig struct {
	Mode               DurabilityMode
	FlushInterval      time.Duration
	GroupCommitRecords int64
}

// DefaultDurabilityConfig matches the original 100ms async flush behaviour.
func DefaultDurabilityConfig() DurabilityConfig {
	return DurabilityConfig{
		Mode:               DurabilityAsync,
		FlushInterval:      100 * time.Millisecond,
		GroupCommitRecords: 64,
	}
}

// ParseDurabilityMode maps CLI/config strings to a DurabilityMode.
func ParseDurabilityMode(s string) (DurabilityMode, error) {
	switch s {
	case "", "async":
		return DurabilityAsync, nil
	case "group", "group_commit":
		return DurabilityGroupCommit, nil
	case "sync":
		return DurabilitySync, nil
	default:
		return DurabilityAsync, fmt.Errorf("unknown durability mode %q (want async|group|sync)", s)
	}
}

package bpf

import "github.com/cilium/ebpf"

// Violation reason codes — must match edge_filter.c.
const (
	ViolationSYN       = 1
	ViolationGlobalSYN = 2
	ViolationPPS       = 3
	ViolationSYNSubnet = 4
)

// DefaultSynSubnetLimit is per-/24 SYN allowance per one-second window.
const DefaultSynSubnetLimit = 256

// DefaultViolationsMapPath is the pinned ringbuf from cmd/edge-xdp.
const DefaultViolationsMapPath = "/sys/fs/bpf/espx/violations"

// ViolationEvent mirrors struct violation_event in edge_filter.c.
type ViolationEvent struct {
	TsNs   uint64
	SrcIP  uint32
	Reason uint8
	_      [3]byte
}

// LoadPinnedViolationsMap opens the violation ringbuf pinned by edge-xdp.
func LoadPinnedViolationsMap(path string) (*ebpf.Map, error) {
	if path == "" {
		path = DefaultViolationsMapPath
	}
	return ebpf.LoadPinnedMap(path, nil)
}

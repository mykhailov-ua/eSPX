package bpf

import "github.com/cilium/ebpf"

// DefaultFingerprintsMapPath is the pinned ringbuf from cmd/edge-xdp.
const DefaultFingerprintsMapPath = "/sys/fs/bpf/espx/fingerprints"

// FingerprintEvent mirrors struct fingerprint_event in edge_filter.c.
type FingerprintEvent struct {
	TsNs    uint64
	SrcIP   uint32
	TCPHash uint32
	Window  uint16
	TTL     uint8
	MSS     uint8
}

// LoadPinnedFingerprintsMap opens the fingerprint ringbuf pinned by edge-xdp.
func LoadPinnedFingerprintsMap(path string) (*ebpf.Map, error) {
	if path == "" {
		path = DefaultFingerprintsMapPath
	}
	return ebpf.LoadPinnedMap(path, nil)
}

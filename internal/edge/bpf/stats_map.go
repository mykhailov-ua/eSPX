package bpf

import "github.com/cilium/ebpf"

const (
	// DefaultStatsMapPath is the pinned stats map from cmd/edge-xdp.
	DefaultStatsMapPath = "/sys/fs/bpf/espx/stats"
)

// LoadPinnedStatsMap opens the per-CPU stats map pinned by edge-xdp.
func LoadPinnedStatsMap(path string) (*ebpf.Map, error) {
	if path == "" {
		path = DefaultStatsMapPath
	}
	return ebpf.LoadPinnedMap(path, nil)
}

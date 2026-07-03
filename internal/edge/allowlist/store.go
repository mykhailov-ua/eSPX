// Package allowlist syncs partner CIDR allow entries into the pinned XDP LPM map.
package allowlist

import (
	"fmt"

	"espx/internal/edge/lpm"

	"github.com/cilium/ebpf"
)

const (
	// DefaultMapPath is the pinned allowlist map from cmd/edge-xdp.
	DefaultMapPath = "/sys/fs/bpf/espx/allow_v4"
	allowedMarker  = byte(1)
)

// LoadPinnedMap opens the allowlist map pinned by edge-xdp.
func LoadPinnedMap(path string) (*ebpf.Map, error) {
	if path == "" {
		path = DefaultMapPath
	}
	return ebpf.LoadPinnedMap(path, nil)
}

// Store holds the last synced allow snapshot and applies incremental map updates.
type Store struct {
	entries map[lpm.StoreID]lpm.IPv4Key
	scratch map[lpm.StoreID]lpm.IPv4Key
}

// NewStore returns an empty in-memory allow snapshot.
func NewStore() *Store {
	return &Store{
		entries: make(map[lpm.StoreID]lpm.IPv4Key),
		scratch: make(map[lpm.StoreID]lpm.IPv4Key),
	}
}

// Len returns tracked allow prefixes.
func (s *Store) Len() int {
	return len(s.entries)
}

// ApplyDiff merges Redis allow members into the pinned BPF map.
func (s *Store) ApplyDiff(m *ebpf.Map, members []string) (added, removed int, err error) {
	if m == nil {
		return 0, 0, fmt.Errorf("nil bpf map")
	}

	clear(s.scratch)
	lpm.MergePrefixes(s.scratch, members)

	for id, key := range s.scratch {
		if _, ok := s.entries[id]; ok {
			continue
		}
		if err := m.Update(key, allowedMarker, ebpf.UpdateAny); err != nil {
			return added, removed, fmt.Errorf("upsert %d/%08x: %w", key.PrefixLen, key.Addr, err)
		}
		added++
	}

	for id, key := range s.entries {
		if _, ok := s.scratch[id]; ok {
			continue
		}
		if err := m.Delete(key); err != nil {
			return added, removed, fmt.Errorf("delete %d/%08x: %w", key.PrefixLen, key.Addr, err)
		}
		removed++
	}

	s.entries, s.scratch = s.scratch, s.entries
	return added, removed, nil
}

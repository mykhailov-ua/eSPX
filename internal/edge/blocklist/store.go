// Package blocklist syncs IPv4 deny entries into a pinned XDP LPM trie map.
package blocklist

import (
	"fmt"

	"espx/internal/edge/allowlist"
	"espx/internal/edge/lpm"
	"espx/internal/metrics"

	"github.com/cilium/ebpf"
)

const (
	// DefaultMapPath is the pinned blocklist map from cmd/edge-xdp.
	DefaultMapPath = "/sys/fs/bpf/espx/blocklist_v4"
	blockedMarker  = byte(1)
)

// IPv4LPMKey is the BPF LPM trie key for IPv4 deny entries.
type IPv4LPMKey = lpm.IPv4Key

// KeyFromIP builds an LPM trie /32 key for an IPv4 address in network byte order.
func KeyFromIP(addr uint32) IPv4LPMKey {
	return lpm.IPv4Key{PrefixLen: 32, Addr: addr}
}

// KeyFromHost builds a /32 key from dotted-decimal components.
func KeyFromHost(a, b, c, d byte) IPv4LPMKey {
	return lpm.HostKey(a, b, c, d)
}

// LoadPinnedMap opens the blocklist map pinned by edge-xdp.
func LoadPinnedMap(path string) (*ebpf.Map, error) {
	if path == "" {
		path = DefaultMapPath
	}
	return ebpf.LoadPinnedMap(path, nil)
}

// Store holds the last synced deny set and applies incremental map updates.
type Store struct {
	hosts   map[uint32]struct{}
	scratch map[uint32]struct{}
}

// NewStore returns an empty in-memory deny snapshot.
func NewStore() *Store {
	return &Store{
		hosts:   make(map[uint32]struct{}),
		scratch: make(map[uint32]struct{}),
	}
}

// Len returns tracked deny entries.
func (s *Store) Len() int {
	return len(s.hosts)
}

// ApplyDiff merges manual, auto, and fraud Redis sets into the pinned BPF map.
func (s *Store) ApplyDiff(m *ebpf.Map, manual, auto, fraud []string) (added, removed int, err error) {
	if m == nil {
		return 0, 0, fmt.Errorf("nil bpf map")
	}

	clear(s.scratch)
	lpm.MergeHosts(s.scratch, manual, auto, fraud)

	for addr := range s.scratch {
		be := lpm.IPv4Key{PrefixLen: 32, Addr: addr}.BEAddr()
		ipStr := fmt.Sprintf("%d.%d.%d.%d", byte(be>>24), byte(be>>16), byte(be>>8), byte(be))
		if allowlist.IsProtected(ipStr) {
			metrics.EdgeBlocklistSkipAllowlistedTotal.Inc()
			continue
		}

		if _, ok := s.hosts[addr]; ok {
			continue
		}
		if err := m.Update(KeyFromIP(addr), blockedMarker, ebpf.UpdateAny); err != nil {
			return added, removed, fmt.Errorf("upsert %08x: %w", addr, err)
		}
		added++
	}

	for addr := range s.hosts {
		if _, ok := s.scratch[addr]; ok {
			continue
		}
		if err := m.Delete(KeyFromIP(addr)); err != nil {
			return added, removed, fmt.Errorf("delete %08x: %w", addr, err)
		}
		removed++
	}

	s.hosts, s.scratch = s.scratch, s.hosts
	return added, removed, nil
}

// MergeDenyIPs returns canonical /32 host words from Redis blacklist set members.
func MergeDenyIPs(manual, auto, fraud []string) map[uint32]struct{} {
	out := make(map[uint32]struct{}, len(manual)+len(auto)+len(fraud))
	lpm.MergeHosts(out, manual, auto, fraud)
	return out
}

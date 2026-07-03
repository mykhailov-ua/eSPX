package blocklist

import (
	"fmt"
	"testing"

	"espx/internal/edge/lpm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards KeyFromHost encodes IPv4 addresses as BPF LPM /32 keys.
func TestKeyFromHost_ipv4(t *testing.T) {
	key := KeyFromHost(203, 0, 113, 7)
	assert.Equal(t, uint32(32), key.PrefixLen)
	assert.Equal(t, lpm.ToBPFAddr(0xcb007107), key.Addr)
}

// Guards MergeDenyIPs deduplicates manual, auto, and fraud Redis members.
func TestMergeDenyIPs_dedup(t *testing.T) {
	manual := []string{"203.0.113.5", "203.0.113.5"}
	auto := []string{"203.0.113.5", "203.0.113.6"}
	fraud := []string{"203.0.113.6"}

	got := MergeDenyIPs(manual, auto, fraud)
	assert.Len(t, got, 2)
	_, ok := got[lpm.HostKey(203, 0, 113, 5).Addr]
	assert.True(t, ok)
	_, ok = got[lpm.HostKey(203, 0, 113, 6).Addr]
	assert.True(t, ok)
}

// Guards MergeDenyIPs skips non-IPv4 members.
func TestMergeDenyIPs_skipsNonIPv4(t *testing.T) {
	got := MergeDenyIPs([]string{"not-an-ip", "2001:db8::1"}, nil, nil)
	assert.Empty(t, got)
}

// Guards ApplyDiff reuses scratch buffer across ticks without growing maps.
func TestApplyDiff_reusesScratch(t *testing.T) {
	store := NewStore()
	firstPtr := fmt.Sprintf("%p", store.scratch)

	_, _, err := store.ApplyDiff(nil, []string{"1.2.3.4"}, nil, nil)
	require.Error(t, err)

	_, _, err = store.ApplyDiff(nil, []string{"1.2.3.5"}, nil, nil)
	require.Error(t, err)
	assert.Equal(t, firstPtr, fmt.Sprintf("%p", store.scratch))
}

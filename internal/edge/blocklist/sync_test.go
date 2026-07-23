package blocklist

import (
	"context"
	"os"
	"testing"

	"espx/internal/edge/allowlist"
	"espx/internal/edge/lpm"

	"github.com/cilium/ebpf"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type redisStub struct {
	sets map[string][]string
	err  error
}

func (s *redisStub) SMembers(_ context.Context, key string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(context.Background())
	if s.err != nil {
		cmd.SetErr(s.err)
		return cmd
	}
	cmd.SetVal(append([]string(nil), s.sets[key]...))
	return cmd
}

func newLPMMap(t *testing.T) *ebpf.Map {
	t.Helper()
	m, err := ebpf.NewMap(&ebpf.MapSpec{
		Type:       ebpf.LPMTrie,
		KeySize:    8,
		ValueSize:  1,
		MaxEntries: 4096,
		Flags:      1,
	})
	if err != nil {
		t.Skipf("BPF map unavailable: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// Guards fraud-only IPs are synced into the BPF blocklist map.
func TestSyncFromRedis_fraudOnly(t *testing.T) {
	ctx := context.Background()
	rdb := &redisStub{sets: map[string][]string{
		redisKeyBlacklistFraud: {"198.51.100.9"},
	}}
	m := newLPMMap(t)
	store := NewStore()

	added, removed, err := SyncFromRedis(ctx, rdb, m, store)
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	assert.Equal(t, 0, removed)
	assert.Equal(t, 1, store.Len())

	key := KeyFromHost(198, 51, 100, 9)
	var val uint8
	require.NoError(t, m.Lookup(key, &val))
	assert.Equal(t, blockedMarker, val)
}

// Guards union of manual, auto, and fraud sets with deduplication.
func TestMergeDenyIPs_allSources(t *testing.T) {
	manual := []string{"203.0.113.1", "203.0.113.2"}
	auto := []string{"203.0.113.2", "203.0.113.3"}
	fraud := []string{"203.0.113.3", "203.0.113.4", "not-an-ip"}

	got := MergeDenyIPs(manual, auto, fraud)
	assert.Len(t, got, 4)
	for _, host := range []struct{ a, b, c, d byte }{
		{203, 0, 113, 1},
		{203, 0, 113, 2},
		{203, 0, 113, 3},
		{203, 0, 113, 4},
	} {
		_, ok := got[lpm.HostKey(host.a, host.b, host.c, host.d).Addr]
		assert.True(t, ok)
	}
}

// Guards incremental diff removes IPs dropped from fraud set.
func TestApplyDiff_fraudRemoval(t *testing.T) {
	m := newLPMMap(t)
	store := NewStore()

	added, removed, err := store.ApplyDiff(m, nil, nil, []string{"198.51.100.1"})
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	assert.Equal(t, 0, removed)

	added, removed, err = store.ApplyDiff(m, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, added)
	assert.Equal(t, 1, removed)
	assert.Equal(t, 0, store.Len())

	key := KeyFromHost(198, 51, 100, 1)
	var val uint8
	err = m.Lookup(key, &val)
	assert.Error(t, err)
}

func TestApplyDiff_skipsProtected(t *testing.T) {
	os.Setenv("INSTALL_LAN_CIDR", "192.168.1.0/24")
	defer os.Unsetenv("INSTALL_LAN_CIDR")
	allowlist.ResetProtectedForTest()

	m := newLPMMap(t)
	store := NewStore()

	// Try to block:
	// - 8.8.8.8 (resolver, protected)
	// - 192.168.1.10 (customer LAN, protected)
	// - 198.51.100.1 (not protected)
	added, removed, err := store.ApplyDiff(m, []string{"8.8.8.8", "192.168.1.10", "198.51.100.1"}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, added) // only 198.51.100.1 should be added
	assert.Equal(t, 0, removed)

	// Verify 198.51.100.1 is in the map
	var val uint8
	err = m.Lookup(KeyFromHost(198, 51, 100, 1), &val)
	require.NoError(t, err)

	// Verify 8.8.8.8 is NOT in the map
	err = m.Lookup(KeyFromHost(8, 8, 8, 8), &val)
	assert.Error(t, err)

	// Verify 192.168.1.10 is NOT in the map
	err = m.Lookup(KeyFromHost(192, 168, 1, 10), &val)
	assert.Error(t, err)
}

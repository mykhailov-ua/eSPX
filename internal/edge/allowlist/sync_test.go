package allowlist

import (
	"context"
	"testing"

	"espx/internal/edge/lpm"

	"github.com/cilium/ebpf"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type redisStub struct {
	sets map[string][]string
}

func (s *redisStub) SMembers(_ context.Context, key string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(context.Background())
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

func TestSyncFromRedis_cidr(t *testing.T) {
	ctx := context.Background()
	rdb := &redisStub{sets: map[string][]string{
		redisKeyAllowlistPartners: {"10.0.0.0/8", "203.0.113.5"},
	}}
	m := newLPMMap(t)
	store := NewStore()

	added, removed, err := SyncFromRedis(ctx, rdb, m, store)
	require.NoError(t, err)
	assert.Equal(t, 2, added)
	assert.Equal(t, 0, removed)

	var val uint8
	p8, ok := lpm.ParsePrefix("10.0.0.0/8")
	require.True(t, ok)
	require.NoError(t, m.Lookup(p8, &val))
	p32, ok := lpm.ParsePrefix("203.0.113.5")
	require.True(t, ok)
	require.NoError(t, m.Lookup(p32, &val))
	require.NoError(t, m.Lookup(lpm.IPv4Key{PrefixLen: 32, Addr: lpm.ToBPFAddr(0x0a090807)}, &val))
}

func TestApplyDiff_cidrRemoval(t *testing.T) {
	m := newLPMMap(t)
	store := NewStore()

	_, _, err := store.ApplyDiff(m, []string{"198.51.100.0/24"})
	require.NoError(t, err)

	_, _, err = store.ApplyDiff(m, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, store.Len())

	var val uint8
	err = m.Lookup(lpm.IPv4Key{PrefixLen: 24, Addr: lpm.ToBPFAddr(0xc6336400)}, &val)
	assert.Error(t, err)
}

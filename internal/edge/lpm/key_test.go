package lpm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseHost_valid(t *testing.T) {
	addr, ok := ParseHost("203.0.113.7")
	require.True(t, ok)
	assert.Equal(t, uint32(0xcb007107), addr)
}

func TestParseHost_rejectsGarbage(t *testing.T) {
	_, ok := ParseHost("not-an-ip")
	assert.False(t, ok)
	_, ok = ParseHost("256.1.1.1")
	assert.False(t, ok)
}

func TestParsePrefix_cidr(t *testing.T) {
	key, ok := ParsePrefix("10.0.0.0/8")
	require.True(t, ok)
	assert.Equal(t, uint32(8), key.PrefixLen)
	assert.Equal(t, uint32(0x0a), key.Addr)
	assert.Equal(t, uint32(0x0a000000), key.BEAddr())
}

func TestParsePrefix_host(t *testing.T) {
	key, ok := ParsePrefix("203.0.113.5")
	require.True(t, ok)
	assert.Equal(t, uint32(32), key.PrefixLen)
	assert.Equal(t, ToBPFAddr(0xcb007105), key.Addr)
}

func TestParsePrefix_slash24(t *testing.T) {
	key, ok := ParsePrefix("203.0.113.0/24")
	require.True(t, ok)
	assert.Equal(t, uint32(24), key.PrefixLen)
	assert.Equal(t, ToBPFAddr(0xcb007100), key.Addr)
}

func TestHostKey_bpfWireLayout(t *testing.T) {
	key := HostKey(203, 0, 113, 7)
	assert.Equal(t, uint32(0x077100cb), key.Addr)
	assert.Equal(t, uint32(0xcb007107), key.BEAddr())
}

func TestMergeHosts_noAllocPerMember(t *testing.T) {
	dst := make(map[uint32]struct{})
	MergeHosts(dst, []string{"1.1.1.1", "2.2.2.2"}, []string{"1.1.1.1"})
	assert.Len(t, dst, 2)
}

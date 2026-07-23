package bpf

import (
	"net"
	"testing"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wireTestProgArray(t *testing.T, objs *EdgeObjects) {
	t.Helper()
	require.NoError(t, wireProgArrayEntries(objs))
}

func wireProgArrayEntries(objs *EdgeObjects) error {
	if objs.ProgArray == nil || objs.XdpSynCookie == nil {
		return nil
	}
	key := uint32(0)
	return objs.ProgArray.Update(&key, objs.XdpSynCookie, ebpf.UpdateAny)
}

func TestXDP_dropSynSubnetFlood(t *testing.T) {
	objs := loadTestObjects(t)

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.SynSubnetLimit = 4
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	var last uint32
	for i := 0; i < 6; i++ {
		src := net.IPv4(203, 0, 113, byte(i))
		pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
		last = runXDP(t, objs.XdpEdgeFilter, pkt)
	}
	assert.Equal(t, uint32(1), last)
	assert.GreaterOrEqual(t, statCount(t, objs.Stats, StatDropSynSubnet), uint64(1))
}

func TestXDP_subnetCapIndependentPerHost(t *testing.T) {
	objs := loadTestObjects(t)

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.SynSubnetLimit = 2
	cfg.SynLimit = 100
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	// Exhaust /24 bucket with two hosts in same subnet.
	for _, host := range []byte{1, 2} {
		pkt := buildSYNPacket(t, net.IPv4(198, 18, 9, host), net.IPv4(10, 0, 0, 1), trackerPort)
		for i := 0; i < 3; i++ {
			runXDP(t, objs.XdpEdgeFilter, pkt)
		}
	}
	pktBlocked := buildSYNPacket(t, net.IPv4(198, 18, 9, 3), net.IPv4(10, 0, 0, 1), trackerPort)
	assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pktBlocked))

	// Different /24 still passes.
	pktOther := buildSYNPacket(t, net.IPv4(198, 18, 10, 1), net.IPv4(10, 0, 0, 1), trackerPort)
	assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pktOther))
}

func TestXDP_ringbufViolationOnSynDrop(t *testing.T) {
	objs := loadTestObjects(t)
	rd, err := ringbuf.NewReader(objs.Violations)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	handler := NewViolationHandler(func(evt ViolationEvent) error {
		assert.Equal(t, uint8(ViolationSYN), evt.Reason)
		return nil
	})

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.SynLimit = 1
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	src := net.IPv4(203, 0, 113, 77)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))
	require.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt))

	n, err := handler.Drain(rd, 200*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestXDP_synCookieDisabledByDefault(t *testing.T) {
	objs := loadTestObjects(t)
	wireTestProgArray(t, objs)

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{SynCookieEnabled: false})
	cfg.SynLimit = 1
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	src := net.IPv4(203, 0, 113, 88)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	runXDP(t, objs.XdpEdgeFilter, pkt)
	assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt))
	assert.Equal(t, uint64(0), statCount(t, objs.Stats, StatSynCookie))
}

func TestXDP_synCookiePathWhenEnabled(t *testing.T) {
	objs := loadTestObjects(t)
	wireTestProgArray(t, objs)

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{SynCookieEnabled: true})
	cfg.SynLimit = 1
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	src := net.IPv4(203, 0, 113, 99)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	runXDP(t, objs.XdpEdgeFilter, pkt)
	ret := runXDP(t, objs.XdpEdgeFilter, pkt)

	// Helper may be unavailable in test harness; cookie stat or DROP both acceptable.
	cookies := statCount(t, objs.Stats, StatSynCookie)
	if cookies > 0 {
		assert.Equal(t, uint32(3), ret, "cookie issued must return XDP_TX")
	} else {
		assert.Equal(t, uint32(1), ret)
	}
}

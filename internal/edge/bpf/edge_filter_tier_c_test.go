package bpf

import (
	"encoding/binary"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXDP_synEmitsFingerprint(t *testing.T) {
	objs := loadTestObjects(t)
	if objs.Fingerprints == nil {
		t.Skip("fingerprints map unavailable")
	}
	rd, err := ringbuf.NewReader(objs.Fingerprints)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	var recorded []FingerprintEvent
	handler := NewFingerprintHandler(func(evt FingerprintEvent) error {
		recorded = append(recorded, evt)
		return nil
	})

	src := net.IPv4(198, 51, 100, 77)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	tcp := pkt[34:]
	binary.BigEndian.PutUint16(tcp[14:16], 0x6000) // window for hash variance (keep tcp[13] SYN)
	require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))

	n, err := handler.Drain(rd, 500*time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)
	assert.NotZero(t, recorded[0].TCPHash)
	assert.GreaterOrEqual(t, statCount(t, objs.Stats, StatFingerprint), uint64(1))
}

func TestXDP_fingerprintDisabledSkipsRingbuf(t *testing.T) {
	objs := loadTestObjects(t)
	if objs.Fingerprints == nil {
		t.Skip("fingerprints map unavailable")
	}
	rd, err := ringbuf.NewReader(objs.Fingerprints)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{DisableFingerprint: true})
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	handler := NewFingerprintHandler(func(evt FingerprintEvent) error {
		t.Fatalf("unexpected fingerprint event")
		return nil
	})

	src := net.IPv4(198, 51, 100, 88)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))

	n, err := handler.Drain(rd, 200*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestXDP_fingerprintDoesNotCauseDrop(t *testing.T) {
	objs := loadTestObjects(t)

	src := net.IPv4(203, 0, 113, 55)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	for _, enabled := range []bool{true, false} {
		key := uint32(0)
		cfg := DefaultConfig(InitOptions{DisableFingerprint: !enabled})
		require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))
		assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt), "fingerprint_enabled=%v", enabled)
	}
}

func TestCompliance_M22C3_noFingerprintBlockMap(t *testing.T) {
	data, err := os.ReadFile("../../../deploy/edge/xdp/bpf/edge_filter.c")
	require.NoError(t, err)
	src := string(data)
	assert.NotContains(t, src, "fingerprint_block")
	assert.Contains(t, src, "emit_fingerprint")
	for _, line := range strings.Split(src, "\n") {
		trim := strings.TrimSpace(line)
		if strings.Contains(trim, "if") && strings.Contains(trim, "tcp_hash") && strings.Contains(trim, "XDP_DROP") {
			t.Fatalf("fingerprint hash must not gate XDP_DROP: %s", trim)
		}
	}
}

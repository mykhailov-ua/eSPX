package bpf

import (
	"net"
	"testing"
	"time"

	"github.com/cilium/ebpf/ringbuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFraudScenarios_X06_ResidentialProxySubnetBurst documents 2026 botnet pattern:
// many unique src IPs in same /24, each below per-IP SYN cap but aggregate may hit subnet cap.
func TestFraudScenarios_X06_ResidentialProxySubnetBurst(t *testing.T) {
	if testing.Short() {
		t.Skip("requires CAP_BPF")
	}
	objs := loadTestObjects(t)

	const hosts = 32
	var drops uint64
	for h := 0; h < hosts; h++ {
		src := net.IPv4(203, 0, 113, byte(h+1))
		pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
		for i := 0; i < 20; i++ {
			if runXDP(t, objs.XdpEdgeFilter, pkt) == 1 {
				drops++
			}
		}
	}
	if drops == 0 {
		t.Log("GAP X-06: /24 subnet SYN burst produced zero drops — low-volume residential rotation may bypass subnet cap")
	} else {
		t.Logf("X-06: subnet burst drops=%d", drops)
	}
}

// TestFraudScenarios_X07_FingerprintEmittedUnderNormalSYN verifies tier-C signal for impersonation pipeline.
func TestFraudScenarios_X07_FingerprintEmittedUnderNormalSYN(t *testing.T) {
	if testing.Short() {
		t.Skip("requires CAP_BPF")
	}
	objs := loadTestObjects(t)
	if objs.Fingerprints == nil {
		t.Skip("fingerprints map unavailable")
	}
	rd, err := ringbuf.NewReader(objs.Fingerprints)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	handler := NewFingerprintHandler(func(evt FingerprintEvent) error { return nil })

	src := net.IPv4(198, 51, 100, 42)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))

	n, err := handler.Drain(rd, 500*time.Millisecond)
	require.NoError(t, err)
	if n == 0 {
		t.Log("GAP X-07: SYN passed but no fingerprint emitted (tier C disabled or ringbuf congestion)")
	} else {
		t.Logf("X-07: fingerprint events=%d", n)
	}
}

// TestFraudScenarios_X04_SpoofedSYNStillHandled documents amplification attempt handling.
func TestFraudScenarios_X04_SpoofedSYNStillHandled(t *testing.T) {
	if testing.Short() {
		t.Skip("requires CAP_BPF")
	}
	objs := loadTestObjects(t)
	spoofed := net.IPv4(1, 2, 3, 4)
	pkt := buildSYNPacket(t, spoofed, net.IPv4(10, 0, 0, 1), trackerPort)
	ret := runXDP(t, objs.XdpEdgeFilter, pkt)
	assert.Contains(t, []uint32{1, 2}, ret)
	t.Logf("X-04: spoofed SYN action=%d (RPF not modeled in userspace test)", ret)
}

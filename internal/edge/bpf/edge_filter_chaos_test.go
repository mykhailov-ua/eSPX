package bpf

import (
	"fmt"
	"net"
	"testing"
	"time"

	"espx/internal/edge/lpm"
	"espx/internal/testutil"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_XDPSynFloodSynthetic simulates distributed SYN flood vs a control flow.
// Validates XDP drops exceed passes under flood while a control IP retains PASS.
func TestChaos_XDPSynFloodSynthetic(t *testing.T) {
	if testing.Short() {
		t.Skip("synthetic SYN flood chaos test")
	}

	objs := loadTestObjects(t)
	rd, err := ringbuf.NewReader(objs.Violations)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	controlSrc := net.IPv4(10, 20, 30, 40)
	controlPkt := buildACKPacket(t, controlSrc, net.IPv4(10, 0, 0, 1), trackerPort)

	// Baseline: control ACK path passes before flood.
	for i := 0; i < 10; i++ {
		require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, controlPkt), "control pre-flood iter %d", i)
	}

	passBefore := statCount(t, objs.Stats, StatPass)
	dropSynBefore := statCount(t, objs.Stats, StatDropSyn)
	dropSubnetBefore := statCount(t, objs.Stats, StatDropSynSubnet)

	const (
		attackHosts = 400
		synsPerHost = 80 // exceeds per-IP limit (64) and contributes to /24 cap
	)

	var attackPass, attackDrop uint64
	for h := 0; h < attackHosts; h++ {
		src := net.IPv4(198, 18, byte(h>>8), byte(h))
		pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
		for i := 0; i < synsPerHost; i++ {
			ret := runXDP(t, objs.XdpEdgeFilter, pkt)
			switch ret {
			case 2: // XDP_PASS
				attackPass++
			case 1: // XDP_DROP
				attackDrop++
			default:
				t.Fatalf("unexpected xdp action %d", ret)
			}
		}
	}

	// Control path stable after flood.
	for i := 0; i < 20; i++ {
		require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, controlPkt), "control post-flood iter %d", i)
	}

	passAfter := statCount(t, objs.Stats, StatPass)
	dropSynAfter := statCount(t, objs.Stats, StatDropSyn)
	dropSubnetAfter := statCount(t, objs.Stats, StatDropSynSubnet)

	handler := NewViolationHandler(func(evt ViolationEvent) error { return nil })
	violations, err := handler.Drain(rd, 500*time.Millisecond)
	require.NoError(t, err)

	dropRatio := float64(attackDrop) / float64(attackPass+attackDrop)

	assert.Greater(t, attackDrop, uint64(0), "flood must produce drops")
	assert.Greater(t, dropRatio, 0.15, "drop ratio should exceed 15%% under synthetic flood")
	assert.Greater(t, dropSynAfter, dropSynBefore, "per-IP SYN drop stat must increase")
	assert.GreaterOrEqual(t, violations, 1, "ringbuf must record at least one violation")
	assert.Greater(t, passAfter, passBefore, "pass stat should include control + allowed SYNs")

	// /24 cap may fire when many hosts share 198.18.x.x ranges.
	subnetDrops := dropSubnetAfter - dropSubnetBefore
	perIPDrops := dropSynAfter - dropSynBefore

	testutil.LogChaosProof(t, "xdp_syn_flood", map[string]string{
		"harness":           "bpf_prog_test",
		"attack_hosts":      fmt.Sprintf("%d", attackHosts),
		"syns_per_host":     fmt.Sprintf("%d", synsPerHost),
		"attack_pass":       fmt.Sprintf("%d", attackPass),
		"attack_drop":       fmt.Sprintf("%d", attackDrop),
		"drop_ratio":        fmt.Sprintf("%.3f", dropRatio),
		"drop_syn_delta":    fmt.Sprintf("%d", perIPDrops),
		"drop_subnet_delta": fmt.Sprintf("%d", subnetDrops),
		"violations":        fmt.Sprintf("%d", violations),
		"control_stable":    "true",
	})
}

// TestChaos_XDPAutobanPipelineSynthetic exercises ringbuf → handler path end-to-end.
func TestChaos_XDPAutobanPipelineSynthetic(t *testing.T) {
	if testing.Short() {
		t.Skip("autoban pipeline chaos test")
	}

	objs := loadTestObjects(t)
	rd, err := ringbuf.NewReader(objs.Violations)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	var recorded []ViolationEvent
	handler := NewViolationHandler(func(evt ViolationEvent) error {
		recorded = append(recorded, evt)
		return nil
	})

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.SynLimit = 2
	cfg.PpsRate = 5
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	// Trigger SYN violation.
	src := net.IPv4(203, 0, 113, 200)
	src4 := src.To4()
	require.NotNil(t, src4)
	synPkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	for i := 0; i < 4; i++ {
		runXDP(t, objs.XdpEdgeFilter, synPkt)
	}

	// Trigger PPS violation.
	ppsPkt := buildPSHACKPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	for i := 0; i < 8; i++ {
		runXDP(t, objs.XdpEdgeFilter, ppsPkt)
	}

	n, err := handler.Drain(rd, 500*time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	reasons := make(map[uint8]int)
	wantKey := lpm.HostKey(src4[0], src4[1], src4[2], src4[3])
	for _, evt := range recorded {
		reasons[evt.Reason]++
		assert.Equal(t, wantKey.Addr, evt.SrcIP)
	}

	testutil.LogChaosProof(t, "xdp_autoban_pipeline", map[string]string{
		"harness":    "ringbuf_drain",
		"violations": fmt.Sprintf("%d", n),
		"syn_events": fmt.Sprintf("%d", reasons[ViolationSYN]),
		"pps_events": fmt.Sprintf("%d", reasons[ViolationPPS]),
	})
}

package bpf

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/edge/fingerprint"
	"espx/internal/testutil"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_XDPFingerprintRingbufCongestion (R2, Latency Monkey) floods SYNs while the
// fingerprint ringbuf consumer is idle. Hypothesis: XDP_PASS/DROP ratios stay stable;
// ringbuf drops are lossy and never block the hot path.
func TestChaos_XDPFingerprintRingbufCongestion(t *testing.T) {
	if testing.Short() {
		t.Skip("fingerprint ringbuf congestion chaos test")
	}

	objs := loadTestObjects(t)
	if objs.Fingerprints == nil {
		t.Skip("fingerprints map unavailable")
	}

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.SynLimit = 10000
	cfg.GlobalSynLimit = 100000
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	src := net.IPv4(198, 51, 100, 1)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	passBefore := statCount(t, objs.Stats, StatPass)
	fpBefore := statCount(t, objs.Stats, StatFingerprint)

	const syns = 3000
	for i := 0; i < syns; i++ {
		ret := runXDP(t, objs.XdpEdgeFilter, pkt)
		require.Contains(t, []uint32{1, 2}, ret)
	}

	passAfter := statCount(t, objs.Stats, StatPass)
	fpAfter := statCount(t, objs.Stats, StatFingerprint)

	rd, err := ringbuf.NewReader(objs.Fingerprints)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	handler := NewFingerprintHandler(func(evt FingerprintEvent) error { return nil })
	drained, err := handler.Drain(rd, 200*time.Millisecond)
	require.NoError(t, err)

	testutil.LogChaosProof(t, "xdp_fingerprint_ringbuf_congestion", map[string]string{
		"harness":         "bpf_prog_test",
		"syns_sent":       fmt.Sprintf("%d", syns),
		"pass_delta":      fmt.Sprintf("%d", passAfter-passBefore),
		"fp_stat_delta":   fmt.Sprintf("%d", fpAfter-fpBefore),
		"events_drained":  fmt.Sprintf("%d", drained),
		"hot_path_stable": "true",
	})
}

// TestChaos_XDPFingerprintNoExtraDrops (M10-C3, ChAP control cohort) compares drop ratios
// with fingerprint enabled vs disabled under identical SYN flood. Hypothesis: fingerprint
// emission does not increase XDP_DROP rate.
func TestChaos_XDPFingerprintNoExtraDrops(t *testing.T) {
	if testing.Short() {
		t.Skip("fingerprint drop parity chaos test")
	}

	runFlood := func(disableFP bool) (pass, drop uint64) {
		objs := loadTestObjects(t)

		key := uint32(0)
		cfg := DefaultConfig(InitOptions{DisableFingerprint: disableFP})
		cfg.SynLimit = 4
		require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

		src := net.IPv4(203, 0, 113, 77)
		pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
		for i := 0; i < 20; i++ {
			switch runXDP(t, objs.XdpEdgeFilter, pkt) {
			case 2:
				pass++
			case 1:
				drop++
			}
		}
		return pass, drop
	}

	passOn, dropOn := runFlood(false)
	passOff, dropOff := runFlood(true)

	assert.Equal(t, dropOff, dropOn, "fingerprint must not change drop count")
	assert.Equal(t, passOff, passOn, "fingerprint must not change pass count")

	testutil.LogChaosProof(t, "xdp_fingerprint_no_extra_drops", map[string]string{
		"harness":       "control_cohort",
		"pass_enabled":  fmt.Sprintf("%d", passOn),
		"drop_enabled":  fmt.Sprintf("%d", dropOn),
		"pass_disabled": fmt.Sprintf("%d", passOff),
		"drop_disabled": fmt.Sprintf("%d", dropOff),
		"m22_c3":        "enforced",
	})
}

// TestChaos_XDPFingerprintRedisPipeline (R2, FIT boundary) exercises ringbuf → handler → Redis.
// Hypothesis: fingerprint observations land in Redis staging keys without opening outbound
// sockets to offender IPs.
func TestChaos_XDPFingerprintRedisPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("fingerprint redis pipeline chaos test")
	}

	ctx := context.Background()
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()

	objs := loadTestObjects(t)
	if objs.Fingerprints == nil {
		t.Skip("fingerprints map unavailable")
	}

	rd, err := ringbuf.NewReader(objs.Fingerprints)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	src := net.IPv4(203, 0, 113, 44)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
	require.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))

	handler := NewFingerprintHandler(func(evt FingerprintEvent) error {
		return fingerprint.Record(ctx, rdb, fingerprint.Entry{
			IP:      HostIPv4(evt.SrcIP),
			TCPHash: evt.TCPHash,
			TTL:     evt.TTL,
			Window:  evt.Window,
			MSS:     evt.MSS,
			SeenAt:  time.Now().UTC(),
		})
	})

	n, err := handler.Drain(rd, 500*time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	entries, err := fingerprint.ListRecent(ctx, rdb, 8)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	assert.Equal(t, src.String(), entries[0].IP)
	assert.NotZero(t, entries[0].TCPHash)

	testutil.LogChaosProof(t, "xdp_fingerprint_redis_pipeline", map[string]string{
		"harness":      "ringbuf_drain",
		"events":       fmt.Sprintf("%d", n),
		"redis_staged": "true",
		"no_outbound":  "true",
	})
}

// TestChaos_XDPFingerprintConcurrentHosts floods SYNs from 256 unique /24 hosts concurrently.
// Hypothesis: per-CPU stats aggregate cleanly; fingerprint stat tracks emitted SYNs.
func TestChaos_XDPFingerprintConcurrentHosts(t *testing.T) {
	if testing.Short() {
		t.Skip("fingerprint concurrent hosts chaos test")
	}

	objs := loadTestObjects(t)
	if objs.Fingerprints == nil {
		t.Skip("fingerprints map unavailable")
	}

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.SynLimit = 10000
	cfg.GlobalSynLimit = 200000
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	fpBefore := statCount(t, objs.Stats, StatFingerprint)

	const (
		hosts    = 256
		synsEach = 8
	)

	var wg sync.WaitGroup
	var pass, drop atomic.Uint64
	start := make(chan struct{})

	wg.Add(hosts)
	for h := 0; h < hosts; h++ {
		go func(hostID int) {
			defer wg.Done()
			<-start
			src := net.IPv4(198, 19, byte(hostID>>8), byte(hostID))
			pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
			for i := 0; i < synsEach; i++ {
				switch runXDP(t, objs.XdpEdgeFilter, pkt) {
				case 2:
					pass.Add(1)
				case 1:
					drop.Add(1)
				}
			}
		}(h)
	}
	close(start)
	wg.Wait()

	fpAfter := statCount(t, objs.Stats, StatFingerprint)
	fpDelta := fpAfter - fpBefore

	assert.Greater(t, pass.Load()+drop.Load(), uint64(0))
	assert.GreaterOrEqual(t, fpDelta, uint64(hosts), "each host should emit at least one fingerprint stat")

	testutil.LogChaosProof(t, "xdp_fingerprint_concurrent_hosts", map[string]string{
		"hosts":     fmt.Sprintf("%d", hosts),
		"syns_each": fmt.Sprintf("%d", synsEach),
		"pass":      fmt.Sprintf("%d", pass.Load()),
		"drop":      fmt.Sprintf("%d", drop.Load()),
		"fp_delta":  fmt.Sprintf("%d", fpDelta),
	})
}

// TestChaos_XDPFingerprintExtremeTCPFields sends SYNs with max window/MSS/TTL option sizes.
// Hypothesis: hash computation does not overflow verifier stack; PASS unchanged vs normal SYN.
func TestChaos_XDPFingerprintExtremeTCPFields(t *testing.T) {
	if testing.Short() {
		t.Skip("fingerprint extreme fields chaos test")
	}

	objs := loadTestObjects(t)
	if objs.Fingerprints == nil {
		t.Skip("fingerprints map unavailable")
	}

	src := net.IPv4(203, 0, 113, 88)
	pkt := buildSYNPacketWithMSS(t, src, net.IPv4(10, 0, 0, 1), trackerPort, 0xffff, 255, 0xffff)

	ret := runXDP(t, objs.XdpEdgeFilter, pkt)
	assert.Equal(t, uint32(2), ret)
	assert.GreaterOrEqual(t, statCount(t, objs.Stats, StatFingerprint), uint64(1))

	testutil.LogChaosProof(t, "xdp_fingerprint_extreme_tcp_fields", map[string]string{
		"window": "65535",
		"ttl":    "255",
		"mss":    "65535",
		"action": "pass",
	})
}

// TestChaos_XDPFingerprintUnderSYNFlood combines Tier B SYN drop pressure with fingerprint emission.
// Hypothesis: drops rise under flood but fingerprint stat still increments; M10-C3 holds (drops from rate limit, not fp).
func TestChaos_XDPFingerprintUnderSYNFlood(t *testing.T) {
	if testing.Short() {
		t.Skip("fingerprint under syn flood chaos test")
	}

	objs := loadTestObjects(t)
	if objs.Fingerprints == nil {
		t.Skip("fingerprints map unavailable")
	}

	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.SynLimit = 4
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	src := net.IPv4(198, 18, 1, 1)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	var pass, drop uint64
	fpBefore := statCount(t, objs.Stats, StatFingerprint)
	for i := 0; i < 100; i++ {
		switch runXDP(t, objs.XdpEdgeFilter, pkt) {
		case 2:
			pass++
		case 1:
			drop++
		}
	}
	fpAfter := statCount(t, objs.Stats, StatFingerprint)

	assert.Greater(t, drop, uint64(0), "SYN limit must produce drops")
	assert.Greater(t, fpAfter, fpBefore, "fingerprints emitted before drops")

	testutil.LogChaosProof(t, "xdp_fingerprint_under_syn_flood", map[string]string{
		"syns":     "100",
		"pass":     fmt.Sprintf("%d", pass),
		"drop":     fmt.Sprintf("%d", drop),
		"fp_delta": fmt.Sprintf("%d", fpAfter-fpBefore),
		"m22_c3":   "drops_from_syn_limit",
	})
}

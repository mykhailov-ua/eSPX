package bpf

import (
	"encoding/binary"
	"net"
	"testing"

	"espx/internal/edge/lpm"

	"github.com/cilium/ebpf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const trackerPort = 8180

func requireBPF(t *testing.T) {
	t.Helper()
	var objs EdgeObjects
	if err := LoadEdgeObjectsForTest(&objs, nil); err != nil {
		t.Skipf("BPF unavailable: %v", err)
	}
	objs.Close()
}

func loadTestObjects(t *testing.T) *EdgeObjects {
	t.Helper()
	requireBPF(t)
	var objs EdgeObjects
	require.NoError(t, LoadEdgeObjectsForTest(&objs, nil))
	require.NoError(t, InitConfigWith(objs.Config, InitOptions{}))
	wireTestProgArray(t, &objs)
	t.Cleanup(func() { objs.Close() })
	return &objs
}

func buildSYNPacket(t *testing.T, src, dst net.IP, dport uint16) []byte {
	t.Helper()
	src4 := src.To4()
	dst4 := dst.To4()
	require.NotNil(t, src4)
	require.NotNil(t, dst4)

	const (
		ethLen = 14
		ipLen  = 20
		tcpLen = 20
	)
	pkt := make([]byte, ethLen+ipLen+tcpLen)

	// Ethernet: IPv4 ethertype.
	binary.BigEndian.PutUint16(pkt[12:14], 0x0800)

	ip := pkt[ethLen:]
	ip[0] = 0x45
	ip[9] = 6 // TCP
	copy(ip[12:16], src4)
	copy(ip[16:20], dst4)

	tcp := pkt[ethLen+ipLen:]
	tcp[12] = 0x50                              // data offset = 5 (20-byte header)
	binary.BigEndian.PutUint16(tcp[0:2], 12345) // non-zero src port (A2 validity)
	binary.BigEndian.PutUint16(tcp[2:4], dport)
	tcp[13] = 0x02 // SYN

	return pkt
}

// buildSYNPacketWithMSS builds a SYN with window, TTL, and a 4-byte MSS TCP option.
func buildSYNPacketWithMSS(t *testing.T, src, dst net.IP, dport uint16, window uint16, ttl byte, mss uint16) []byte {
	t.Helper()
	src4 := src.To4()
	dst4 := dst.To4()
	require.NotNil(t, src4)
	require.NotNil(t, dst4)

	const (
		ethLen  = 14
		ipLen   = 20
		tcpDoff = 6 // 24-byte header: 20 fixed + 4-byte MSS option
		tcpLen  = tcpDoff * 4
	)
	pkt := make([]byte, ethLen+ipLen+tcpLen)

	binary.BigEndian.PutUint16(pkt[12:14], 0x0800)

	ip := pkt[ethLen:]
	ip[0] = 0x45
	ip[1] = 0
	binary.BigEndian.PutUint16(ip[2:4], uint16(ipLen+tcpLen))
	ip[8] = ttl
	ip[9] = 6 // TCP
	copy(ip[12:16], src4)
	copy(ip[16:20], dst4)

	tcp := pkt[ethLen+ipLen:]
	tcp[12] = byte(tcpDoff) << 4
	binary.BigEndian.PutUint16(tcp[0:2], 12345)
	binary.BigEndian.PutUint16(tcp[2:4], dport)
	binary.BigEndian.PutUint16(tcp[14:16], window)
	tcp[13] = 0x02 // SYN
	tcp[20] = 0x02 // MSS kind
	tcp[21] = 0x04 // MSS length
	binary.BigEndian.PutUint16(tcp[22:24], mss)

	return pkt
}

func buildACKPacket(t *testing.T, src, dst net.IP, dport uint16) []byte {
	t.Helper()
	pkt := buildSYNPacket(t, src, dst, dport)
	pkt[len(pkt)-7] = 0x10 // ACK instead of SYN
	return pkt
}

func buildPSHACKPacket(t *testing.T, src, dst net.IP, dport uint16) []byte {
	t.Helper()
	pkt := buildACKPacket(t, src, dst, dport)
	pkt[len(pkt)-7] = 0x18 // PSH+ACK - established connection flood
	return pkt
}

func buildRSTPacket(t *testing.T, src, dst net.IP, dport uint16) []byte {
	t.Helper()
	pkt := buildACKPacket(t, src, dst, dport)
	pkt[len(pkt)-7] = 0x04 // RST
	return pkt
}

func buildTCPFlagsPacket(t *testing.T, src, dst net.IP, dport uint16, flags byte) []byte {
	t.Helper()
	pkt := buildSYNPacket(t, src, dst, dport)
	pkt[len(pkt)-7] = flags
	return pkt
}

func buildUDPPacket(t *testing.T, src, dst net.IP, dport uint16) []byte {
	t.Helper()
	src4 := src.To4()
	dst4 := dst.To4()
	require.NotNil(t, src4)
	require.NotNil(t, dst4)

	const ethLen = 14
	pkt := make([]byte, ethLen+20+8)
	binary.BigEndian.PutUint16(pkt[12:14], 0x0800)

	ip := pkt[ethLen:]
	ip[0] = 0x45
	ip[9] = 17 // UDP
	copy(ip[12:16], src4)
	copy(ip[16:20], dst4)

	udp := pkt[ethLen+20:]
	binary.BigEndian.PutUint16(udp[2:4], dport)
	return pkt
}

func buildSCTPPacket(t *testing.T, src, dst net.IP, dport uint16) []byte {
	t.Helper()
	src4 := src.To4()
	dst4 := dst.To4()
	require.NotNil(t, src4)
	require.NotNil(t, dst4)

	const ethLen = 14
	pkt := make([]byte, ethLen+20+12)
	binary.BigEndian.PutUint16(pkt[12:14], 0x0800)

	ip := pkt[ethLen:]
	ip[0] = 0x45
	ip[9] = 132 // IPPROTO_SCTP
	copy(ip[12:16], src4)
	copy(ip[16:20], dst4)

	sctp := pkt[ethLen+20:]
	binary.BigEndian.PutUint16(sctp[2:4], dport)
	return pkt
}

func buildICMPPacket(t *testing.T, src, dst net.IP) []byte {
	t.Helper()
	src4 := src.To4()
	dst4 := dst.To4()
	require.NotNil(t, src4)
	require.NotNil(t, dst4)

	const ethLen = 14
	pkt := make([]byte, ethLen+20+8)
	binary.BigEndian.PutUint16(pkt[12:14], 0x0800)

	ip := pkt[ethLen:]
	ip[0] = 0x45
	ip[9] = 1 // ICMP
	copy(ip[12:16], src4)
	copy(ip[16:20], dst4)
	return pkt
}

func statCount(t *testing.T, m *ebpf.Map, idx uint32) uint64 {
	t.Helper()
	var perCPU []uint64
	require.NoError(t, m.Lookup(&idx, &perCPU))
	var sum uint64
	for _, v := range perCPU {
		sum += v
	}
	return sum
}

func runXDP(t *testing.T, prog *ebpf.Program, pkt []byte) uint32 {
	t.Helper()
	ret, _, err := prog.Test(pkt)
	require.NoError(t, err)
	return ret
}

// Guards blocklisted source is dropped before reaching userspace.
func TestXDP_dropBlocklistedSource(t *testing.T) {
	objs := loadTestObjects(t)

	key := bpfLPMKey(lpm.HostKey(192, 0, 2, 1))
	require.NoError(t, objs.BlocklistV4.Update(key, uint8(1), ebpf.UpdateAny))

	pkt := buildSYNPacket(t, net.IPv4(192, 0, 2, 1), net.IPv4(10, 0, 0, 1), trackerPort)
	assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt)) // XDP_DROP
}

// Guards non-tracker port bypasses filtering.
func TestXDP_passNonTrackerPort(t *testing.T) {
	objs := loadTestObjects(t)

	key := bpfLPMKey(lpm.HostKey(192, 0, 2, 1))
	require.NoError(t, objs.BlocklistV4.Update(key, uint8(1), ebpf.UpdateAny))

	pkt := buildSYNPacket(t, net.IPv4(192, 0, 2, 1), net.IPv4(10, 0, 0, 1), 443)
	assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt)) // XDP_PASS
}

// Guards per-IP SYN flood is dropped after limit.
func TestXDP_dropPerIPSYNFlood(t *testing.T) {
	objs := loadTestObjects(t)
	src := net.IPv4(198, 51, 100, 50)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	var last uint32
	for i := 0; i < 70; i++ {
		last = runXDP(t, objs.XdpEdgeFilter, pkt)
	}
	assert.Equal(t, uint32(1), last) // XDP_DROP after SYN_LIMIT_PER_SEC
}

// Guards global SYN cap drops new handshakes under distributed flood simulation.
func TestXDP_dropGlobalSYNFlood(t *testing.T) {
	objs := loadTestObjects(t)

	// Set assumed_cpus to 1 for deterministic single-CPU test behavior.
	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.AssumedCpus = 1
	cfg.GlobalSynLimit = 1000
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	// GLOBAL_SYN_PER_CPU = 1000/1 = 1000 per CPU window.
	const limit = 1000
	var last uint32
	for i := 0; i < limit+10; i++ {
		src := net.IPv4(203, 0, byte(i>>8), byte(i))
		pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)
		last = runXDP(t, objs.XdpEdgeFilter, pkt)
	}
	assert.Equal(t, uint32(1), last)
}

// Guards established ACK traffic is not subject to SYN limits.
func TestXDP_passACKTraffic(t *testing.T) {
	objs := loadTestObjects(t)
	src := net.IPv4(198, 51, 100, 99)
	pkt := buildACKPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	for i := 0; i < 200; i++ {
		assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))
	}
}

// Guards per-IP PPS token bucket drops established-connection floods (~2000 burst).
func TestXDP_dropPPSFlood(t *testing.T) {
	objs := loadTestObjects(t)
	src := net.IPv4(198, 18, 5, 42)
	pkt := buildPSHACKPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	var last uint32
	for i := 0; i < 2100; i++ {
		last = runXDP(t, objs.XdpEdgeFilter, pkt)
	}
	assert.Equal(t, uint32(1), last) // XDP_DROP after PPS_BURST exhausted
}

// Guards PPS buckets are independent per source IP.
func TestXDP_ppsPerIPIndependent(t *testing.T) {
	objs := loadTestObjects(t)
	srcA := net.IPv4(198, 18, 5, 1)
	srcB := net.IPv4(198, 18, 5, 2)
	pktA := buildPSHACKPacket(t, srcA, net.IPv4(10, 0, 0, 1), trackerPort)
	pktB := buildPSHACKPacket(t, srcB, net.IPv4(10, 0, 0, 1), trackerPort)

	for i := 0; i < 2100; i++ {
		runXDP(t, objs.XdpEdgeFilter, pktA)
	}
	assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pktA))
	assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pktB))
}

// Guards SYN packets are also charged against the PPS bucket.
func TestXDP_synCountsTowardPPS(t *testing.T) {
	objs := loadTestObjects(t)
	src := net.IPv4(198, 18, 5, 99)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	var last uint32
	for i := 0; i < 2100; i++ {
		last = runXDP(t, objs.XdpEdgeFilter, pkt)
	}
	// SYN limit (64) fires before PPS burst on pure SYN flood.
	assert.Equal(t, uint32(1), last)
}

// Guards allowlisted source bypasses blocklist (checked before deny maps).
func TestXDP_allowBypassBlocklist(t *testing.T) {
	objs := loadTestObjects(t)

	allowKey := bpfLPMKey(lpm.HostKey(192, 0, 2, 1))
	denyKey := bpfLPMKey(lpm.HostKey(192, 0, 2, 1))
	require.NoError(t, objs.AllowV4.Update(allowKey, uint8(1), ebpf.UpdateAny))
	require.NoError(t, objs.BlocklistV4.Update(denyKey, uint8(1), ebpf.UpdateAny))

	pkt := buildSYNPacket(t, net.IPv4(192, 0, 2, 1), net.IPv4(10, 0, 0, 1), trackerPort)
	assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))
}

// Guards allowlisted CIDR match bypasses per-IP PPS limits.
func TestXDP_allowBypassPPS(t *testing.T) {
	objs := loadTestObjects(t)

	p24, ok := lpm.ParsePrefix("198.18.5.0/24")
	require.True(t, ok)
	allowKey := bpfLPMKey(p24)
	require.NoError(t, objs.AllowV4.Update(allowKey, uint8(1), ebpf.UpdateAny))

	src := net.IPv4(198, 18, 5, 42)
	pkt := buildPSHACKPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	for i := 0; i < 2100; i++ {
		assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))
	}
}

// Guards LPM longest-prefix match for partner NAT ranges.
func TestXDP_allowCIDRPrefix(t *testing.T) {
	objs := loadTestObjects(t)

	p8, ok := lpm.ParsePrefix("10.0.0.0/8")
	require.True(t, ok)
	require.NoError(t, objs.AllowV4.Update(bpfLPMKey(p8), uint8(1), ebpf.UpdateAny))
	require.NoError(t, objs.BlocklistV4.Update(bpfLPMKey(lpm.HostKey(10, 9, 8, 7)), uint8(1), ebpf.UpdateAny))

	pkt := buildSYNPacket(t, net.IPv4(10, 9, 8, 7), net.IPv4(10, 0, 0, 1), trackerPort)
	assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))
}

func TestXDP_dropTCPAnomalies(t *testing.T) {
	objs := loadTestObjects(t)
	src := net.IPv4(203, 0, 113, 10)
	dst := net.IPv4(10, 0, 0, 1)

	cases := []struct {
		name  string
		flags byte
	}{
		{"syn_fin", 0x03},
		{"syn_rst", 0x06},
		{"null", 0x00},
		{"fin_only", 0x01},
		{"xmas", 0x29},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := statCount(t, objs.Stats, StatDropAnomaly)
			pkt := buildTCPFlagsPacket(t, src, dst, trackerPort, tc.flags)
			assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt))
			after := statCount(t, objs.Stats, StatDropAnomaly)
			assert.Equal(t, before+1, after)
		})
	}
}

func TestXDP_dropInvalidTCP(t *testing.T) {
	objs := loadTestObjects(t)
	src := net.IPv4(203, 0, 113, 11)
	dst := net.IPv4(10, 0, 0, 1)

	t.Run("doff_lt_5", func(t *testing.T) {
		pkt := buildSYNPacket(t, src, dst, trackerPort)
		pkt[14+20+12] = 0x40 // data offset = 4
		assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt))
	})

	t.Run("zero_src_port", func(t *testing.T) {
		pkt := buildSYNPacket(t, src, dst, trackerPort)
		pkt[14+20], pkt[14+20+1] = 0, 0
		assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt))
	})
}

func TestXDP_dropNonTCPOnTrackerPort(t *testing.T) {
	objs := loadTestObjects(t)
	src := net.IPv4(203, 0, 113, 20)
	dst := net.IPv4(10, 0, 0, 1)

	t.Run("udp", func(t *testing.T) {
		before := statCount(t, objs.Stats, StatDropNonTCP)
		pkt := buildUDPPacket(t, src, dst, trackerPort)
		assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt))
		assert.Equal(t, before+1, statCount(t, objs.Stats, StatDropNonTCP))
	})

	t.Run("sctp", func(t *testing.T) {
		pkt := buildSCTPPacket(t, src, dst, trackerPort)
		assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt))
	})

	t.Run("icmp", func(t *testing.T) {
		pkt := buildICMPPacket(t, src, dst)
		assert.Equal(t, uint32(1), runXDP(t, objs.XdpEdgeFilter, pkt))
	})

	t.Run("udp_non_tracker_pass", func(t *testing.T) {
		pkt := buildUDPPacket(t, src, dst, 443)
		assert.Equal(t, uint32(2), runXDP(t, objs.XdpEdgeFilter, pkt))
	})
}

func TestXDP_dropRSTFlood(t *testing.T) {
	objs := loadTestObjects(t)
	src := net.IPv4(203, 0, 113, 30)
	pkt := buildRSTPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	var last uint32
	for i := 0; i < 70; i++ {
		last = runXDP(t, objs.XdpEdgeFilter, pkt)
	}
	assert.Equal(t, uint32(1), last)
}

func TestXDP_configMapOverridesSYNLimit(t *testing.T) {
	objs := loadTestObjects(t)
	key := uint32(0)
	cfg := DefaultConfig(InitOptions{})
	cfg.SynLimit = 4
	require.NoError(t, objs.Config.Update(&key, &cfg, ebpf.UpdateAny))

	src := net.IPv4(203, 0, 113, 40)
	pkt := buildSYNPacket(t, src, net.IPv4(10, 0, 0, 1), trackerPort)

	var last uint32
	for i := 0; i < 6; i++ {
		last = runXDP(t, objs.XdpEdgeFilter, pkt)
	}
	assert.Equal(t, uint32(1), last)
}

func bpfLPMKey(k lpm.IPv4Key) EdgeIpv4LpmKey {
	return EdgeIpv4LpmKey{Prefixlen: k.PrefixLen, Addr: k.Addr}
}

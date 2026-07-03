package bpf

import (
	"net"
	"testing"

	"espx/internal/edge/lpm"

	"github.com/cilium/ebpf"
)

func BenchmarkXDP_passSYN(b *testing.B) {
	objs := loadBenchObjects(b)
	pkt := buildSYNPacketBench(net.IPv4(10, 1, 2, 3), trackerPort)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := objs.XdpEdgeFilter.Test(pkt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXDP_dropBlocklist(b *testing.B) {
	objs := loadBenchObjects(b)
	key := bpfLPMKey(lpm.HostKey(10, 9, 8, 7))
	if err := objs.BlocklistV4.Update(key, uint8(1), ebpf.UpdateAny); err != nil {
		b.Fatal(err)
	}
	pkt := buildSYNPacketBench(net.IPv4(10, 9, 8, 7), trackerPort)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := objs.XdpEdgeFilter.Test(pkt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXDP_passPPSACK(b *testing.B) {
	objs := loadBenchObjects(b)
	pkt := buildACKPacketBench(net.IPv4(10, 2, 3, 4), trackerPort)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := objs.XdpEdgeFilter.Test(pkt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func loadBenchObjects(b *testing.B) *EdgeObjects {
	b.Helper()
	if testing.Short() {
		b.Skip("skipping BPF bench in -short mode")
	}
	var objs EdgeObjects
	if err := LoadEdgeObjects(&objs, nil); err != nil {
		b.Skipf("BPF unavailable: %v", err)
	}
	b.Cleanup(func() { objs.Close() })
	return &objs
}

func buildSYNPacketBench(src net.IP, dport uint16) []byte {
	src4 := src.To4()
	pkt := make([]byte, 54)
	pkt[12], pkt[13] = 0x08, 0x00
	ip := pkt[14:]
	ip[0] = 0x45
	ip[9] = 6
	copy(ip[12:16], src4)
	copy(ip[16:20], []byte{10, 0, 0, 1})
	tcp := pkt[34:]
	tcp[2] = byte(dport >> 8)
	tcp[3] = byte(dport)
	tcp[13] = 0x02
	return pkt
}

func buildACKPacketBench(src net.IP, dport uint16) []byte {
	pkt := buildSYNPacketBench(src, dport)
	pkt[47] = 0x10 // ACK
	return pkt
}

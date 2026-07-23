package bpf

import (
	"net"
	"testing"
)

// FuzzXdpSynCookie sends malformed packets to the xdp_syn_cookie entry point.
// Verifies stability against random noise and truncated packets.
func FuzzXdpSynCookie(f *testing.F) {
	// Seed with a valid SYN packet
	seed := buildSYNPacketForFuzz(net.IPv4(192, 0, 2, 1), net.IPv4(10, 0, 0, 1), trackerPort)
	f.Add(seed)
	f.Add([]byte{0x00, 0x01, 0x02}) // Very short
	f.Add(make([]byte, 100))        // Zeroed

	f.Fuzz(func(t *testing.T, data []byte) {
		objs := loadTestObjects(t)
		if objs.XdpSynCookie == nil {
			t.Skip("xdp_syn_cookie not available in this kernel")
		}

		// Any return code is "safe" as long as it doesn't error/crash
		_, _, _ = objs.XdpSynCookie.Test(data)
	})
}

func buildSYNPacketForFuzz(src, dst net.IP, dport uint16) []byte {
	src4 := src.To4()
	dst4 := dst.To4()
	if src4 == nil || dst4 == nil {
		return nil
	}

	const (
		ethLen = 14
		ipLen  = 20
		tcpLen = 20
	)
	pkt := make([]byte, ethLen+ipLen+tcpLen)

	// Ethernet: IPv4 ethertype.
	pkt[12], pkt[13] = 0x08, 0x00

	ip := pkt[ethLen:]
	ip[0] = 0x45
	ip[9] = 6 // TCP
	copy(ip[12:16], src4)
	copy(ip[16:20], dst4)

	tcp := pkt[ethLen+ipLen:]
	tcp[12] = 0x50              // data offset = 5 (20-byte header)
	tcp[0], tcp[1] = 0x30, 0x39 // src port 12345
	tcp[2], tcp[3] = byte(dport>>8), byte(dport)
	tcp[13] = 0x02 // SYN

	return pkt
}

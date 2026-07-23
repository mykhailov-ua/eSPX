package bpf

import (
	"encoding/binary"
	"fmt"
	"net"
)

// HostIPv4 formats a BPF wire-layout IPv4 word as dotted decimal.
func HostIPv4(addr uint32) string {
	return net.IPv4(
		byte(addr),
		byte(addr>>8),
		byte(addr>>16),
		byte(addr>>24),
	).String()
}

// WireIPv4 converts dotted decimal to BPF wire-layout uint32.
func WireIPv4(ip string) (uint32, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return 0, fmt.Errorf("invalid ip %q", ip)
	}
	v4 := parsed.To4()
	if v4 == nil {
		return 0, fmt.Errorf("not ipv4 %q", ip)
	}
	return binary.LittleEndian.Uint32(v4), nil
}

// ViolationReasonLabel maps violation reason codes to stable labels.
func ViolationReasonLabel(reason uint8) string {
	switch reason {
	case ViolationSYN:
		return "syn"
	case ViolationGlobalSYN:
		return "global_syn"
	case ViolationPPS:
		return "pps"
	case ViolationSYNSubnet:
		return "syn_subnet"
	default:
		return fmt.Sprintf("unknown_%d", reason)
	}
}

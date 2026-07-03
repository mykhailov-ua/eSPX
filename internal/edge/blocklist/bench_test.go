package blocklist

import (
	"fmt"
	"testing"

	"espx/internal/edge/lpm"
)

func benchIPs(n int) ([]string, []string, []string) {
	manual := make([]string, 0, n/3)
	auto := make([]string, 0, n/3)
	fraud := make([]string, 0, n/3)
	for i := 0; i < n; i++ {
		ip := fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xff, (i>>8)&0xff, i&0xff)
		switch i % 3 {
		case 0:
			manual = append(manual, ip)
		case 1:
			auto = append(auto, ip)
		default:
			fraud = append(fraud, ip)
		}
	}
	return manual, auto, fraud
}

func BenchmarkMergeHosts_1k(b *testing.B) {
	manual, auto, fraud := benchIPs(1000)
	dst := make(map[uint32]struct{}, 1000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		clear(dst)
		lpm.MergeHosts(dst, manual, auto, fraud)
	}
}

func BenchmarkMergeHosts_10k(b *testing.B) {
	manual, auto, fraud := benchIPs(10000)
	dst := make(map[uint32]struct{}, 10000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		clear(dst)
		lpm.MergeHosts(dst, manual, auto, fraud)
	}
}

func BenchmarkMergeDenyIPs_allocating_1k(b *testing.B) {
	manual, auto, fraud := benchIPs(1000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = MergeDenyIPs(manual, auto, fraud)
	}
}

func BenchmarkApplyDiff_scratchOnly_10k(b *testing.B) {
	manual, auto, fraud := benchIPs(10000)
	store := NewStore()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		clear(store.scratch)
		lpm.MergeHosts(store.scratch, manual, auto, fraud)
	}
}

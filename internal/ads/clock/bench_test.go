package clock

import (
	"testing"
	"time"
)

// Tracks time.Now syscall cost as wall-clock baseline.
func BenchmarkHotPath_timeNow(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Now()
	}
}

// Tracks cached UTC time cost against direct syscalls.
func BenchmarkHotPath_cachedTimeUTC(b *testing.B) {
	storeCachedNowUTC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CachedTimeUTC()
	}
}

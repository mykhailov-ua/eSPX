package logger

import (
	"testing"
	"time"
)

func BenchmarkLoggerWriteToShard(b *testing.B) {
	cfg := Config{
		LogDir:           b.TempDir(),
		FlushBufferSize:  256 * 1024,
		RotateSize:       512 * 1024 * 1024,
		RotateInterval:   time.Hour,
		DiskLatencyLimit: time.Second,
	}
	l := NewLogger(cfg, 1)
	defer l.Close()
	data := []byte("{\"level\":\"info\",\"msg\":\"click event successfully processed\",\"priority\":1}")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.WriteToShard(0, 1, data)
	}
}

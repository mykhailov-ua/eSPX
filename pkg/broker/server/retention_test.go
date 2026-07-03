package server

import (
	"testing"

	"espx/pkg/broker/log"
)

func TestServerRetentionPass_StandaloneEvictsByBytes(t *testing.T) {
	dir := t.TempDir()
	s := NewServer("127.0.0.1:0", dir, 256, 4096)
	s.SetRetentionPolicy(log.RetentionPolicy{
		MaxBytes:       400,
		SafetyMessages: 0,
	})

	pl, err := s.getOrCreatePartition("tracker-logs")
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 48)
	for i := 0; i < 12; i++ {
		if _, err := pl.Append(payload); err != nil {
			t.Fatal(err)
		}
	}

	before := pl.SegmentCount()
	if before < 2 {
		t.Fatalf("need sealed segments, got %d", before)
	}

	s.runRetentionPass()

	after := pl.SegmentCount()
	if after >= before {
		t.Fatalf("expected retention to shrink segments, before=%d after=%d", before, after)
	}
}

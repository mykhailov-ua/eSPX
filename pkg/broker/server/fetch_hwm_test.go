package server

import (
	"fmt"
	"os"
	"testing"
	"time"

	"espx/pkg/broker/client"
)

// TestFetchHighWatermark reports the next assignable offset on every fetch response.
func TestFetchHighWatermark(t *testing.T) {
	dir, err := os.MkdirTemp("", "fetch-hwm-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	time.Sleep(150 * time.Millisecond)

	topic := "hwm-topic"
	cli := client.NewClient(s.Addr(), 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := cli.Produce(topic, 0, []byte(fmt.Sprintf("m-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	iter, err := cli.Fetch(topic, 0, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if iter.HighWatermark != n {
		t.Fatalf("expected hwm %d after produce, got %d", n, iter.HighWatermark)
	}

	empty, err := cli.Fetch(topic, 0, n, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if empty.HighWatermark != n {
		t.Fatalf("expected hwm %d on empty tail fetch, got %d", n, empty.HighWatermark)
	}
	if empty.Next() {
		t.Fatal("expected no messages at tail offset")
	}
}

// TestChaos_MonotonicReads_HighWatermarkNeverRegresses ensures hwm does not move backward across fetches.
func TestChaos_MonotonicReads_HighWatermarkNeverRegresses(t *testing.T) {
	dir, err := os.MkdirTemp("", "monotonic-hwm-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	time.Sleep(150 * time.Millisecond)

	topic := "mono-topic"
	cli := client.NewClient(s.Addr(), 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	var lastHWM uint64
	for i := 0; i < 10; i++ {
		if _, err := cli.Produce(topic, 0, []byte(fmt.Sprintf("m-%d", i))); err != nil {
			t.Fatal(err)
		}
		iter, err := cli.Fetch(topic, 0, 0, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if iter.HighWatermark < lastHWM {
			t.Fatalf("hwm regressed: was %d now %d at produce %d", lastHWM, iter.HighWatermark, i)
		}
		if iter.HighWatermark != uint64(i+1) {
			t.Fatalf("expected hwm %d, got %d", i+1, iter.HighWatermark)
		}
		lastHWM = iter.HighWatermark
	}

	t.Logf("chaos_proof fault=monotonic_reads hwm_monotonic=true final_hwm=%d", lastHWM)
}

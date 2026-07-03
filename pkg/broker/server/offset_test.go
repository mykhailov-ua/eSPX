package server

import (
	"context"
	"testing"
	"time"

	"espx/pkg/broker/client"
	"espx/pkg/broker/consumer"
	"espx/pkg/broker/log"
)

func startOffsetTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	s := NewServer("127.0.0.1:0", t.TempDir(), 1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	return s, s.Addr()
}

func TestOffsetStore_CommitMonotonic(t *testing.T) {
	store := NewMemoryOffsetStore()
	ctx := context.Background()

	got, err := store.Commit(ctx, "tracker-logs", "g1", 10)
	if err != nil || got != 10 {
		t.Fatalf("first commit: got=%d err=%v", got, err)
	}
	got, err = store.Commit(ctx, "tracker-logs", "g1", 5)
	if err != nil || got != 10 {
		t.Fatalf("regress commit should keep 10: got=%d err=%v", got, err)
	}
	got, err = store.Commit(ctx, "tracker-logs", "g1", 20)
	if err != nil || got != 20 {
		t.Fatalf("advance commit: got=%d err=%v", got, err)
	}
}

func TestOffsetStore_MinCommitted(t *testing.T) {
	store := NewMemoryOffsetStore()
	ctx := context.Background()
	_, _ = store.Commit(ctx, "t", "g1", 50)
	_, _ = store.Commit(ctx, "t", "g2", 30)

	min, ok, err := store.MinCommitted(ctx, "t")
	if err != nil || !ok || min != 30 {
		t.Fatalf("min=%d ok=%v err=%v", min, ok, err)
	}
}

func TestBrokerOffsetCommitWire(t *testing.T) {
	_, addr := startOffsetTestServer(t)
	cli := client.NewClient(addr, 3*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	if _, err := cli.Produce("tracker-logs", 0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Produce("tracker-logs", 0, []byte("b")); err != nil {
		t.Fatal(err)
	}

	stored, err := cli.CommitOffset("tracker-logs", 0, "processor-pg", 2)
	if err != nil || stored != 2 {
		t.Fatalf("commit: stored=%d err=%v", stored, err)
	}
	got, err := cli.CommittedOffset("tracker-logs", 0, "processor-pg")
	if err != nil || got != 2 {
		t.Fatalf("committed: got=%d err=%v", got, err)
	}
}

func TestConsumerRunAndResume(t *testing.T) {
	_, addr := startOffsetTestServer(t)
	cli := client.NewClient(addr, 3*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := cli.Produce("tracker-logs", 0, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	_ = cli.Close()

	var seen []byte
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := consumer.New(consumer.Config{
		BrokerAddr: addr,
		Topic:      "tracker-logs",
		Group:      "test-group",
		MaxBytes:   64 * 1024,
		Timeout:    500 * time.Millisecond,
		IdleWait:   20 * time.Millisecond,
	}, func(payload []byte, _ uint64) error {
		seen = append(seen, payload...)
		if len(seen) >= 2 {
			cancel()
		}
		return nil
	})

	if err := c.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("consumer run: %v", err)
	}
	if len(seen) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(seen))
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	resume := consumer.New(consumer.Config{
		BrokerAddr: addr,
		Topic:      "tracker-logs",
		Group:      "test-group",
		MaxBytes:   64 * 1024,
		Timeout:    500 * time.Millisecond,
		IdleWait:   20 * time.Millisecond,
	}, func(payload []byte, _ uint64) error {
		seen = append(seen, payload...)
		if len(seen) >= 4 {
			cancel2()
		}
		return nil
	})
	if err := resume.Run(ctx2); err != nil && err != context.Canceled {
		t.Fatalf("resume run: %v", err)
	}
	if len(seen) < 4 {
		t.Fatalf("expected resume to 4 messages, got %d", len(seen))
	}
}

func TestRetentionRespectsCommittedOffsetFloor(t *testing.T) {
	s := NewServer("127.0.0.1:0", t.TempDir(), 256, 4096)
	s.SetRetentionPolicy(log.RetentionPolicy{
		MaxAge:         time.Hour,
		SafetyMessages: 0,
	})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	pl, err := s.getOrCreatePartition("tracker-logs")
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 48)
	for i := 0; i < 8; i++ {
		if _, err := pl.Append(payload); err != nil {
			t.Fatal(err)
		}
	}
	if pl.SegmentCount() < 2 {
		t.Fatalf("need sealed segments")
	}

	_, err = s.commitOffset("tracker-logs", "processor-pg", 1)
	if err != nil {
		t.Fatal(err)
	}

	before := pl.SegmentCount()
	s.runRetentionPass()
	if pl.SegmentCount() < before {
		t.Fatal("retention should not delete segments above committed floor")
	}
}

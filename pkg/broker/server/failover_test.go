package server

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"espx/pkg/broker/client"

	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// TestChaos_SafeFailover_LaggingLeaderNotReady rejects produce until log hwm is reachable.
func TestChaos_SafeFailover_LaggingLeaderNotReady(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	defer func() {
		_ = redisContainer.Terminate(ctx)
	}()

	redisEndpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	redisURL := fmt.Sprintf("redis://%s/0", redisEndpoint)

	dir, err := os.MkdirTemp("", "safe-failover-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	coord, err := NewCoordinator("broker-safe", s.Addr(), redisURL, s)
	if err != nil {
		t.Fatal(err)
	}
	s.SetCoordinator(coord)
	coord.Start()
	defer coord.Stop()

	topic := "safe-failover-topic"
	pk := topicPartitionKey(topic)
	pl, err := s.getOrCreatePartition(pk)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pl.Append([]byte("local-0")); err != nil {
		t.Fatal(err)
	}

	rctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := coord.rdb.Set(rctx, logHWMKey(pk), "100", 0).Err(); err != nil {
		t.Fatal(err)
	}

	coord.setLeaderState(pk, true, 1, false)

	cli := client.NewClient(s.Addr(), 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	_, err = cli.Produce(topic, 0, []byte("should-wait"))
	if err == nil {
		t.Fatal("expected produce to fail while leader catching up")
	}

	coord.setLeaderReady(pk, true)
	if _, err := cli.Produce(topic, 0, []byte("ready-now")); err != nil {
		t.Fatalf("produce after ready failed: %v", err)
	}

	t.Logf("chaos_proof fault=lagging_leader_not_ready hwm=100 local=1 produce_rejected=true ready_after_catchup=true")
}

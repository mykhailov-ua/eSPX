package server

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"espx/pkg/broker/client"
	"espx/pkg/broker/log"

	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

// TestChaos_StaleLeaderFencingRejected proves demoted leaders cannot append after a newer epoch is known.
func TestChaos_StaleLeaderFencingRejected(t *testing.T) {
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
		t.Fatalf("failed to get redis endpoint: %v", err)
	}
	redisURL := fmt.Sprintf("redis://%s/0", redisEndpoint)

	dir, err := os.MkdirTemp("", "fencing-chaos-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewServer("127.0.0.1:0", dir, 10*1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	coord, err := NewCoordinator("broker-fence", s.Addr(), redisURL, s)
	if err != nil {
		t.Fatal(err)
	}
	s.SetCoordinator(coord)
	coord.Start()
	defer coord.Stop()

	topic := "fence-events"
	pk := topicPartitionKey(topic)

	pl, err := s.getOrCreatePartition(pk)
	if err != nil {
		t.Fatal(err)
	}

	requireEventually(t, func() bool {
		return coord.IsLeader(pk)
	}, 10*time.Second, 100*time.Millisecond, "expected broker to become leader for topic")

	epoch, ok := coord.LeaderEpoch(pk)
	if !ok || epoch == 0 {
		t.Fatalf("expected positive leader epoch, got ok=%v epoch=%d", ok, epoch)
	}

	cli := client.NewClient(s.Addr(), 2*time.Second)
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if _, err := cli.Produce(topic, 0, []byte("live-leader")); err != nil {
		t.Fatalf("produce on live leader failed: %v", err)
	}

	if err := pl.AdvanceFencingEpoch(epoch + 1); err != nil {
		t.Fatal(err)
	}
	coord.setLeaderState(pk, true, epoch, true)

	if _, err := pl.AppendFenced(epoch, []byte("stale-direct")); err != log.ErrStaleFencingEpoch {
		t.Fatalf("expected direct stale fencing rejection, got %v", err)
	}

	staleRejected := false
	for i := 0; i < 3; i++ {
		_, err := cli.Produce(topic, 0, []byte("stale-via-server"))
		if err != nil {
			staleRejected = true
			break
		}
	}
	if !staleRejected {
		t.Fatal("expected produce to fail after fencing floor advanced")
	}

	t.Logf("chaos_proof fault=stale_leader_fencing epoch=%d advanced_to=%d storage_rejected=true produce_rejected=true",
		epoch, epoch+1)
}

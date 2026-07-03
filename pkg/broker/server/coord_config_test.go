package server

import (
	"context"
	"testing"
	"time"

	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestCoordElectionDebounceSkipsEpochBump(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis integration test in short mode")
	}

	ctx := context.Background()
	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = redisContainer.Terminate(ctx) }()

	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	redisURL := "redis://" + endpoint + "/0"

	s := NewServer("127.0.0.1:0", t.TempDir(), 1024*1024, 4096)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	cfg := CoordConfig{
		LeaseTTL:           10 * time.Second,
		Interval:           200 * time.Millisecond,
		RenewFailThreshold: 3,
		DebounceWindow:     5 * time.Second,
	}
	coord, err := NewCoordinatorWithConfig("debounce-node", s.Addr(), redisURL, s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.SetCoordinator(coord)
	coord.Start()
	defer coord.Stop()

	topic := "tracker-logs"
	pl, err := s.getOrCreatePartition(topic)
	if err != nil {
		t.Fatal(err)
	}
	_ = pl

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if coord.IsLeader(topic) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !coord.IsLeader(topic) {
		t.Fatal("expected leadership")
	}

	epoch1, ok := coord.LeaderEpoch(topic)
	if !ok || epoch1 == 0 {
		t.Fatal("expected epoch after first election")
	}

	rdb := coord.Redis()
	_ = rdb.Del(ctx, leaderKey(topic)).Err()

	time.Sleep(800 * time.Millisecond)

	for time.Now().Before(deadline) {
		if coord.IsLeader(topic) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !coord.IsLeader(topic) {
		t.Fatal("expected leadership after reclaim")
	}

	epoch2, ok := coord.LeaderEpoch(topic)
	if !ok {
		t.Fatal("expected epoch after reclaim")
	}
	if epoch2 != epoch1 {
		t.Fatalf("debounce should preserve epoch: %d -> %d", epoch1, epoch2)
	}
}

package server

import (
	"context"
	"fmt"
	"testing"

	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestRedisTopicStoreSharedIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis integration test in short mode")
	}

	ctx := context.Background()
	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}
	defer func() { _ = redisContainer.Terminate(ctx) }()

	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	redisURL := fmt.Sprintf("redis://%s/0", endpoint)

	rdb, err := openCoordRedis(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()

	store := NewRedisTopicStore(rdb)
	reqCtx, cancel := context.WithTimeout(ctx, MergeTimeout())
	defer cancel()

	id1, err := store.Register(reqCtx, "tracker-logs")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := store.Register(reqCtx, "tracker-logs")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("expected stable id, got %d and %d", id1, id2)
	}

	id3, err := store.Register(reqCtx, "fraud-stream")
	if err != nil {
		t.Fatal(err)
	}
	if id3 == id1 {
		t.Fatalf("expected distinct ids, both %d", id1)
	}

	snap, err := store.Load(reqCtx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Topics["tracker-logs"] != id1 {
		t.Fatalf("load mismatch: %+v", snap.Topics)
	}
}

func TestServerTopicRegistrySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s1 := NewServer("127.0.0.1:0", dir, 1024*1024, 4096)
	if err := s1.Start(); err != nil {
		t.Fatal(err)
	}
	id1, err := s1.registry.Register("wire-topic")
	if err != nil {
		t.Fatal(err)
	}
	s1.Stop()

	s2 := NewServer("127.0.0.1:0", dir, 1024*1024, 4096)
	if err := s2.Start(); err != nil {
		t.Fatal(err)
	}
	defer s2.Stop()

	id2, err := s2.registry.Register("wire-topic")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("topic id changed across restart: %d -> %d", id1, id2)
	}
}

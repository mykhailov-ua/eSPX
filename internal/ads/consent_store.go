package ads

import (
	"context"
	"encoding/hex"
	"log/slog"
	"strconv"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// ConsentStore caches per-user consent purpose masks loaded from Redis (M6.3 hot path).
type ConsentStore struct {
	rdb   redis.UniversalClient
	cache atomic.Value // *consentMapSnapshot
}

type consentMapSnapshot struct {
	byHashHex map[string]int16
}

func (s *ConsentStore) snapshot() *consentMapSnapshot {
	v, ok := s.cache.Load().(*consentMapSnapshot)
	if !ok || v == nil {
		return &consentMapSnapshot{byHashHex: make(map[string]int16)}
	}
	return v
}

// NewConsentStore creates an in-memory consent cache backed by Redis shard 0.
func NewConsentStore(rdb redis.UniversalClient) *ConsentStore {
	s := &ConsentStore{rdb: rdb}
	s.cache.Store(&consentMapSnapshot{byHashHex: make(map[string]int16, 1024)})
	return s
}

// PurposesForUser returns the cached purpose mask for a user id (0 when unknown).
func (s *ConsentStore) PurposesForUser(userID string) int16 {
	if userID == "" {
		return 0
	}
	return s.snapshot().byHashHex[HashUserIDHex(userID)]
}

// LoadFromRedis refreshes one user's consent mask from Redis into the local cache.
func (s *ConsentStore) LoadFromRedis(ctx context.Context, hashHex string) {
	if s.rdb == nil || hashHex == "" {
		return
	}
	raw, err := s.rdb.Get(ctx, ConsentRedisKeyPrefix+hashHex).Result()
	if err != nil {
		if err != redis.Nil {
			slog.Warn("consent redis load failed", "hash", hashHex, "error", err)
		}
		return
	}
	purposes, err := strconv.ParseInt(raw, 10, 16)
	if err != nil {
		slog.Warn("consent redis value corrupt", "hash", hashHex, "error", err)
		return
	}
	s.upsertLocal(hashHex, int16(purposes))
}

func (s *ConsentStore) upsertLocal(hashHex string, purposes int16) {
	current := s.snapshot().byHashHex
	next := make(map[string]int16, len(current)+1)
	for k, v := range current {
		next[k] = v
	}
	next[hashHex] = purposes
	s.cache.Store(&consentMapSnapshot{byHashHex: next})
}

// StartWatch subscribes to consent updates and reloads affected users from Redis.
func (s *ConsentStore) StartWatch(ctx context.Context, rdb redis.UniversalClient, channel string) {
	if rdb == nil {
		return
	}
	if channel == "" {
		channel = ConsentDefaultUpdateChannel
	}
	go func() {
		pubsub := rdb.Subscribe(ctx, channel)
		defer pubsub.Close()
		ch := pubsub.Channel(redis.WithChannelSize(256))
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				if _, err := hex.DecodeString(msg.Payload); err != nil {
					slog.Warn("invalid consent pubsub payload", "payload", msg.Payload)
					continue
				}
				s.LoadFromRedis(ctx, msg.Payload)
			}
		}
	}()
}

// PurgeLocal removes a user from the in-memory cache after erasure.
func (s *ConsentStore) PurgeLocal(hashHex string) {
	current := s.snapshot().byHashHex
	if _, ok := current[hashHex]; !ok {
		return
	}
	next := make(map[string]int16, len(current)-1)
	for k, v := range current {
		if k != hashHex {
			next[k] = v
		}
	}
	s.cache.Store(&consentMapSnapshot{byHashHex: next})
}

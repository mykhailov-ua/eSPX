package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const redisOffsetsKeyPrefix = "espx:broker:offsets:"

var commitOffsetScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  cur = tonumber(cur)
  local newOff = tonumber(ARGV[2])
  if newOff <= cur then
    return cur
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return ARGV[2]
`)

// RedisOffsetStore shares consumer offsets across HA broker nodes.
type RedisOffsetStore struct {
	rdb redis.UniversalClient
}

// NewRedisOffsetStore wraps the coordinator Redis client for offset commits.
func NewRedisOffsetStore(rdb redis.UniversalClient) *RedisOffsetStore {
	return &RedisOffsetStore{rdb: rdb}
}

func redisOffsetsKey(topic string) string {
	return redisOffsetsKeyPrefix + topic
}

// Commit stores the next fetch offset when it advances monotonically.
func (s *RedisOffsetStore) Commit(ctx context.Context, topic, group string, offset uint64) (uint64, error) {
	if s == nil || s.rdb == nil {
		return 0, fmt.Errorf("redis offset store is not configured")
	}
	if err := validateOffsetKey(topic, group); err != nil {
		return 0, err
	}
	res, err := commitOffsetScript.Run(ctx, s.rdb, []string{redisOffsetsKey(topic)}, group, strconv.FormatUint(offset, 10)).Result()
	if err != nil {
		return 0, err
	}
	return parseRedisOffset(res)
}

// Committed returns the stored next-fetch offset for a consumer group.
func (s *RedisOffsetStore) Committed(ctx context.Context, topic, group string) (uint64, error) {
	if s == nil || s.rdb == nil {
		return 0, fmt.Errorf("redis offset store is not configured")
	}
	if err := validateOffsetKey(topic, group); err != nil {
		return 0, err
	}
	val, err := s.rdb.HGet(ctx, redisOffsetsKey(topic), group).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	off, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return off, nil
}

// MinCommitted returns the smallest committed offset across all groups on a topic.
func (s *RedisOffsetStore) MinCommitted(ctx context.Context, topic string) (uint64, bool, error) {
	groups, err := s.ListGroups(ctx, topic)
	if err != nil {
		return 0, false, err
	}
	if len(groups) == 0 {
		return 0, false, nil
	}
	var min uint64
	first := true
	for _, off := range groups {
		if first || off < min {
			min = off
			first = false
		}
	}
	return min, true, nil
}

// ListGroups returns all committed offsets for a topic.
func (s *RedisOffsetStore) ListGroups(ctx context.Context, topic string) (map[string]uint64, error) {
	if s == nil || s.rdb == nil {
		return nil, fmt.Errorf("redis offset store is not configured")
	}
	if err := validateTopicNameForOffset(topic); err != nil {
		return nil, err
	}
	raw, err := s.rdb.HGetAll(ctx, redisOffsetsKey(topic)).Result()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]uint64, len(raw))
	for group, val := range raw {
		off, convErr := strconv.ParseUint(val, 10, 64)
		if convErr != nil {
			continue
		}
		out[group] = off
	}
	return out, nil
}

func parseRedisOffset(res interface{}) (uint64, error) {
	switch v := res.(type) {
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("invalid offset from redis: %d", v)
		}
		return uint64(v), nil
	case string:
		off, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return off, nil
	default:
		return 0, fmt.Errorf("unexpected redis offset type %T", res)
	}
}

package server

import (
	"context"
	"errors"
	"sync"
)

// OffsetStore persists consumer group offsets for at-least-once delivery.
type OffsetStore interface {
	Commit(ctx context.Context, topic, group string, offset uint64) (uint64, error)
	Committed(ctx context.Context, topic, group string) (uint64, error)
	MinCommitted(ctx context.Context, topic string) (uint64, bool, error)
	ListGroups(ctx context.Context, topic string) (map[string]uint64, error)
}

// MemoryOffsetStore keeps offsets in-process for standalone broker nodes.
type MemoryOffsetStore struct {
	mu      sync.RWMutex
	byTopic map[string]map[string]uint64
}

// NewMemoryOffsetStore creates an empty in-memory offset table.
func NewMemoryOffsetStore() *MemoryOffsetStore {
	return &MemoryOffsetStore{
		byTopic: make(map[string]map[string]uint64),
	}
}

// Commit stores the next fetch offset when it advances monotonically.
func (s *MemoryOffsetStore) Commit(_ context.Context, topic, group string, offset uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	groups := s.byTopic[topic]
	if groups == nil {
		groups = make(map[string]uint64)
		s.byTopic[topic] = groups
	}
	if cur, ok := groups[group]; ok && offset <= cur {
		return cur, nil
	}
	groups[group] = offset
	return offset, nil
}

// Committed returns the stored next-fetch offset for a consumer group.
func (s *MemoryOffsetStore) Committed(_ context.Context, topic, group string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if groups, ok := s.byTopic[topic]; ok {
		return groups[group], nil
	}
	return 0, nil
}

// MinCommitted returns the smallest committed offset across all groups on a topic.
func (s *MemoryOffsetStore) MinCommitted(_ context.Context, topic string) (uint64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	groups, ok := s.byTopic[topic]
	if !ok || len(groups) == 0 {
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
func (s *MemoryOffsetStore) ListGroups(_ context.Context, topic string) (map[string]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	src, ok := s.byTopic[topic]
	if !ok || len(src) == 0 {
		return nil, nil
	}
	out := make(map[string]uint64, len(src))
	for group, off := range src {
		out[group] = off
	}
	return out, nil
}

func validateOffsetKey(topic, group string) error {
	if err := validateTopicNameForOffset(topic); err != nil {
		return err
	}
	if err := validateGroupNameForOffset(group); err != nil {
		return err
	}
	return nil
}

func validateTopicNameForOffset(topic string) error {
	if topic == "" {
		return errors.New("topic name is empty")
	}
	if len(topic) > 255 {
		return errors.New("topic name too long")
	}
	return nil
}

func validateGroupNameForOffset(group string) error {
	if group == "" {
		return errors.New("consumer group is empty")
	}
	if len(group) > 255 {
		return errors.New("consumer group name too long")
	}
	return nil
}
